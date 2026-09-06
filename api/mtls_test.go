package api

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// testCA generates a self-signed CA and a leaf certificate signed by it,
// entirely in-process (no openssl dependency), for exercising mutual TLS.
type testCA struct {
	caPool     *x509.CertPool
	leafCert   tls.Certificate
	caTemplate *x509.Certificate
	caKey      *ecdsa.PrivateKey
	caDER      []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key failed: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-cluster-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating CA cert failed: %v", err)
	}

	pool := x509.NewCertPool()
	caCertParsed, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parsing CA cert failed: %v", err)
	}
	pool.AddCert(caCertParsed)

	return &testCA{caPool: pool, caTemplate: caTemplate, caKey: caKey, caDER: caDER, leafCert: signLeaf(t, caTemplate, caKey, "test-leaf")}
}

// signLeaf issues a new leaf certificate signed by this CA -- used both
// for "the trusted client" and, with a fresh, un-added-to-any-pool CA,
// for "an untrusted client presenting a cert nobody trusts".
func signLeaf(t *testing.T, caTemplate *x509.Certificate, caKey *ecdsa.PrivateKey, cn string) tls.Certificate {
	t.Helper()
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating leaf key failed: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"127.0.0.1"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating leaf cert failed: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshaling leaf key failed: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("building tls.Certificate failed: %v", err)
	}
	return cert
}

// TestMutualTLSRequiresValidClientCert is the regression test for mutual
// TLS: a server started via StartTLSWithConfig with ClientAuth:
// RequireAndVerifyClientCert must accept a client presenting a
// certificate signed by the trusted CA, and reject one presenting no
// certificate at all, or one signed by a different (untrusted) CA.
func TestMutualTLSRequiresValidClientCert(t *testing.T) {
	ca := newTestCA(t)
	untrustedCA := newTestCA(t) // a different CA the server does not trust

	hs := newTestServer(t)
	server := httptest.NewUnstartedServer(hs.mux)
	server.TLS = &tls.Config{
		ClientCAs:  ca.caPool,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
	server.StartTLS()
	defer server.Close()

	// Trust httptest's own auto-generated server certificate (unrelated
	// to our test CA, which only signs client certs here).
	serverCAPool := x509.NewCertPool()
	serverCAPool.AddCert(server.Certificate())

	doRequest := func(clientCerts []tls.Certificate) (int, error) {
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs:      serverCAPool,
					Certificates: clientCerts,
				},
			},
		}
		resp, err := client.Get(server.URL + "/health")
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		return resp.StatusCode, nil
	}

	// A client presenting a cert signed by the trusted CA must succeed.
	code, err := doRequest([]tls.Certificate{ca.leafCert})
	if err != nil {
		t.Fatalf("request with trusted client cert failed: %v", err)
	}
	if code != http.StatusOK {
		t.Errorf("expected 200 with trusted client cert, got %d", code)
	}

	// A client presenting no certificate at all must be rejected at the
	// TLS layer (the handshake itself fails, so this surfaces as a
	// request error, not an HTTP status code).
	_, err = doRequest(nil)
	if err == nil {
		t.Error("expected TLS handshake to fail for a client presenting no certificate")
	}

	// A client presenting a certificate signed by a CA the server does
	// NOT trust must also be rejected.
	_, err = doRequest([]tls.Certificate{untrustedCA.leafCert})
	if err == nil {
		t.Error("expected TLS handshake to fail for a client certificate signed by an untrusted CA")
	}
}
