package network

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"knative.dev/control-protocol/pkg/certificates"
)

const (
	baseCertsPath = "/etc/control-secret"

	publicCertPath = baseCertsPath + "/" + certificates.SecretCertKey
	pkPath         = baseCertsPath + "/" + certificates.SecretPKKey
	caCertPath     = baseCertsPath + "/" + certificates.SecretCaCertKey
)

func LoadServerTLSConfigFromFile() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(publicCertPath, pkPath)
	if err != nil {
		return nil, err
	}

	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCert)
	conf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   certificates.FakeDnsName,
	}

	return conf, nil
}
