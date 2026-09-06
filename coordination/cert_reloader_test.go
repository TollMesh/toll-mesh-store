package coordination

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeTestCert generates a fresh self-signed cert/key pair with the
// given CommonName and writes it to certFile/keyFile -- used to simulate
// "a PKI rotated this node's certificate" by writing a second, different
// cert to the same paths.
func writeTestCert(t *testing.T, certFile, keyFile, commonName string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key failed: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating cert failed: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0644); err != nil {
		t.Fatalf("writing cert file failed: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key failed: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("writing key file failed: %v", err)
	}
}

// TestCertReloaderPicksUpRotatedCertWithoutRestart is the regression test
// for cert rotation: writing a new cert/key pair to the same file paths
// (simulating what a real PKI rotation system does) must cause the next
// GetCertificate/GetClientCertificate call to serve the new certificate,
// with no process restart and no explicit reload call.
func TestCertReloaderPicksUpRotatedCertWithoutRestart(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "node.crt")
	keyFile := filepath.Join(dir, "node.key")

	writeTestCert(t, certFile, keyFile, "cert-v1")

	reloader, err := NewCertReloader(certFile, keyFile)
	if err != nil {
		t.Fatalf("NewCertReloader failed: %v", err)
	}

	cert, err := reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate failed: %v", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parsing leaf failed: %v", err)
	}
	if leaf.Subject.CommonName != "cert-v1" {
		t.Fatalf("expected initial cert to be cert-v1, got %s", leaf.Subject.CommonName)
	}

	// Ensure a distinguishable mtime -- some filesystems have 1s mtime
	// resolution, and the reload check is mtime-based.
	time.Sleep(1100 * time.Millisecond)
	writeTestCert(t, certFile, keyFile, "cert-v2")

	cert, err = reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate after rotation failed: %v", err)
	}
	leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parsing rotated leaf failed: %v", err)
	}
	if leaf.Subject.CommonName != "cert-v2" {
		t.Fatalf("expected rotated cert to be cert-v2, got %s -- reloader did not pick up the on-disk change", leaf.Subject.CommonName)
	}

	// GetClientCertificate must serve the same up-to-date certificate.
	clientCert, err := reloader.GetClientCertificate(nil)
	if err != nil {
		t.Fatalf("GetClientCertificate failed: %v", err)
	}
	leaf, err = x509.ParseCertificate(clientCert.Certificate[0])
	if err != nil {
		t.Fatalf("parsing client leaf failed: %v", err)
	}
	if leaf.Subject.CommonName != "cert-v2" {
		t.Fatalf("expected GetClientCertificate to also serve cert-v2, got %s", leaf.Subject.CommonName)
	}
}

// TestCertReloaderKeepsServingLastGoodCertOnTransientReadError verifies a
// reload attempt that fails (here, by deleting the key file, simulating a
// PKI mid-rewrite) does not disrupt already-established service -- the
// reloader keeps serving the last successfully loaded certificate rather
// than erroring out an in-flight handshake.
func TestCertReloaderKeepsServingLastGoodCertOnTransientReadError(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "node.crt")
	keyFile := filepath.Join(dir, "node.key")

	writeTestCert(t, certFile, keyFile, "cert-v1")
	reloader, err := NewCertReloader(certFile, keyFile)
	if err != nil {
		t.Fatalf("NewCertReloader failed: %v", err)
	}

	// Simulate a rotation tool mid-write: touch the cert's mtime forward
	// but leave the key file mismatched/removed.
	time.Sleep(1100 * time.Millisecond)
	if err := os.Remove(keyFile); err != nil {
		t.Fatalf("removing key file failed: %v", err)
	}
	now := time.Now()
	os.Chtimes(certFile, now, now)

	cert, err := reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate must not error on a transient reload failure, got: %v", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parsing leaf failed: %v", err)
	}
	if leaf.Subject.CommonName != "cert-v1" {
		t.Fatalf("expected reloader to keep serving the last good cert-v1 during a transient failure, got %s", leaf.Subject.CommonName)
	}
}
