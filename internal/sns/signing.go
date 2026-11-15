package sns

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/url"
	"time"
)

const SigningCertPath = "/sns/SimpleNotificationService.pem"

type signer struct {
	key  *rsa.PrivateKey
	cert []byte
}

func newSigner() (*signer, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "Ginistack local SNS"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	data, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	return &signer{key: key, cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: data})}, nil
}

func (s *service) writeCertificate(w http.ResponseWriter) {
	signing, err := s.signing()
	if err != nil {
		http.Error(w, "Unable to generate the local signing certificate.", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-pem-file")
	_, _ = w.Write(signing.cert)
}

func requestOrigin(r *http.Request) string {
	origin := url.URL{Scheme: "http", Host: r.Host}
	if r.TLS != nil {
		origin.Scheme = "https"
	}
	return origin.String()
}
