package sqs

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"net/http"
)

func newBodyCipher() cipher.AEAD {
	var key [32]byte
	rand.Read(key[:])
	block, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		panic(err)
	}
	return aead
}

func (m *message) sealBody(aead cipher.AEAD) {
	m.encryptedBody = aead.Seal(nil, nil, []byte(m.body), nil)
	m.body = ""
}

func (m *message) openBody(aead cipher.AEAD) (string, *apiError) {
	if m.encryptedBody == nil {
		return m.body, nil
	}
	body, err := aead.Open(nil, nil, m.encryptedBody, nil)
	if err != nil {
		return "", &apiError{"InternalError", "Unable to decrypt the message body.", http.StatusInternalServerError}
	}
	return string(body), nil
}
