package coordination

import (
	"crypto/tls"
	"os"
	"sync"
	"time"
)

// CertReloader watches a certificate/key file pair and reloads them from
// disk whenever their contents actually change (detected via mtime),
// without requiring a process restart. This matters for any real
// deployment using short-lived certificates from an automated PKI/
// rotation system (the common case once TLS is turned on for real) --
// without it, a rotated certificate on disk would sit unused until the
// next restart, silently defeating the point of short-lived certs.
// Safe for concurrent use; GetCertificate and GetClientCertificate are
// both meant to be wired directly into a *tls.Config.
type CertReloader struct {
	certFile, keyFile string

	mu        sync.RWMutex
	cert      *tls.Certificate
	certModAt time.Time
	keyModAt  time.Time
}

// NewCertReloader loads the certificate once immediately (so startup
// still fails fast on a bad cert/key pair, exactly like the previous
// one-shot tls.LoadX509KeyPair call did) and returns a reloader that
// re-checks both files' modification times on every GetCertificate/
// GetClientCertificate call, only re-parsing when something has actually
// changed -- so the steady-state cost (no rotation happening) is one
// stat() per TLS handshake, not a re-parse of the certificate.
func NewCertReloader(certFile, keyFile string) (*CertReloader, error) {
	cr := &CertReloader{certFile: certFile, keyFile: keyFile}
	if err := cr.reload(); err != nil {
		return nil, err
	}
	return cr, nil
}

func (cr *CertReloader) reload() error {
	certInfo, err := os.Stat(cr.certFile)
	if err != nil {
		return err
	}
	keyInfo, err := os.Stat(cr.keyFile)
	if err != nil {
		return err
	}

	cert, err := tls.LoadX509KeyPair(cr.certFile, cr.keyFile)
	if err != nil {
		return err
	}

	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.cert = &cert
	cr.certModAt = certInfo.ModTime()
	cr.keyModAt = keyInfo.ModTime()
	return nil
}

// maybeReload reloads from disk only if either file's mtime has changed
// since the last successful load. A stat() failure, or a LoadX509KeyPair
// failure (e.g. the files are mid-rewrite by whatever's rotating them,
// so the pair is briefly mismatched), is deliberately not surfaced as an
// error here -- this keeps serving the last successfully loaded
// certificate rather than failing an in-flight TLS handshake over a
// transient, self-correcting write race.
func (cr *CertReloader) maybeReload() {
	certInfo, err := os.Stat(cr.certFile)
	if err != nil {
		return
	}
	keyInfo, err := os.Stat(cr.keyFile)
	if err != nil {
		return
	}

	cr.mu.RLock()
	changed := !certInfo.ModTime().Equal(cr.certModAt) || !keyInfo.ModTime().Equal(cr.keyModAt)
	cr.mu.RUnlock()

	if changed {
		_ = cr.reload()
	}
}

// GetCertificate implements the signature tls.Config.GetCertificate
// expects, for a server that should hot-reload its own certificate.
func (cr *CertReloader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	cr.maybeReload()
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.cert, nil
}

// GetClientCertificate implements the signature
// tls.Config.GetClientCertificate expects, for a client (mutual TLS)
// that should hot-reload the certificate it presents to peers.
func (cr *CertReloader) GetClientCertificate(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cr.maybeReload()
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.cert, nil
}
