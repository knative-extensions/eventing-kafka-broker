package config

import (
	"context"
	"fmt"
	"hash/crc32"
	"strconv"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"

	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing-kafka/pkg/common/constants"
)

const (
	TlsEnabled   = "tls.enabled"
	TlsCacert    = "ca.crt"
	TlsUsercert  = "user.crt"
	TlsUserkey   = "user.key"
	SaslUser     = "user"
	SaslType     = "saslType"
	SaslPassword = "password"
)

// parseTls allows backward-compatibility with older consolidated channel secrets
func parseTls(secret *corev1.Secret, kafkaAuthConfig *client.KafkaAuthConfig) {

	// self-signed CERTs we need CA CERT, USER CERT and KEY
	if string(secret.Data[TlsCacert]) != "" {
		// We have a self-signed TLS cert
		tls := &client.KafkaTlsConfig{
			Cacert:   string(secret.Data[TlsCacert]),
			Usercert: string(secret.Data[TlsUsercert]),
			Userkey:  string(secret.Data[TlsUserkey]),
		}
		kafkaAuthConfig.TLS = tls
	} else {
		// Public CERTS from a proper CA do not need this,
		// we can just say `tls.enabled: true`
		tlsEnabled, err := strconv.ParseBool(string(secret.Data[TlsEnabled]))
		if err != nil {
			tlsEnabled = false
		}
		if tlsEnabled {
			// Looks like TLS is desired/enabled:
			kafkaAuthConfig.TLS = &client.KafkaTlsConfig{}
		}
	}
}

func ConfigmapDataCheckSum(configMapData map[string]string) string {
	if configMapData == nil {
		return ""
	}
	configMapDataStr := fmt.Sprintf("%v", configMapData)
	checksum := fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(configMapDataStr)))
	return checksum
}

// GetAuthConfigFromKubernetes Looks Up And Returns Kafka Auth ConfigAnd Brokers From Named Secret
func GetAuthConfigFromKubernetes(ctx context.Context, secretName string, secretNamespace string) *client.KafkaAuthConfig {
	secrets := kubeclient.Get(ctx).CoreV1().Secrets(secretNamespace)
	secret, err := secrets.Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		// A nonexistent secret returns a nil KafkaAuthConfig, which the Sarama builder
		// will interpret as "no authentication needed"
		return nil
	}
	return GetAuthConfigFromSecret(secret)
}

// GetAuthConfigFromSecret Looks Up And Returns Kafka Auth Config And Brokers From Provided Secret
func GetAuthConfigFromSecret(secret *corev1.Secret) *client.KafkaAuthConfig {
	if secret == nil || secret.Data == nil {
		return nil
	}

	username := string(secret.Data[constants.KafkaSecretKeyUsername])
	saslType := string(secret.Data[constants.KafkaSecretKeySaslType])
	var authConfig client.KafkaAuthConfig
	// Backwards-compatibility - Support old consolidated secret fields if present
	// (TLS data is now in the configmap, e.g. sarama.Config.Net.TLS.Config.RootPEMs)
	_, hasTlsCaCert := secret.Data[TlsCacert]
	_, hasTlsEnabled := secret.Data[TlsEnabled]
	if hasTlsEnabled || hasTlsCaCert {
		parseTls(secret, &authConfig)
		username = string(secret.Data[SaslUser])
		saslType = string(secret.Data[SaslType]) // old "saslType" is different than new "sasltype"
	}

	// If we don't convert the empty string to the "PLAIN" default, the client.HasSameSettings()
	// function will assume that they should be treated as differences and needlessly reconfigure
	if saslType == "" {
		saslType = sarama.SASLTypePlaintext
	}

	authConfig.SASL = &client.KafkaSaslConfig{
		User:     username,
		Password: string(secret.Data[constants.KafkaSecretKeyPassword]),
		SaslType: saslType,
	}

	return &authConfig
}

// JoinStringMaps returns a new map containing the contents of both argument maps, preferring the contents of map1 on conflict.
func JoinStringMaps(map1 map[string]string, map2 map[string]string) map[string]string {
	resultMap := make(map[string]string, len(map1))
	for map1Key, map1Value := range map1 {
		resultMap[map1Key] = map1Value
	}
	for map2Key, map2Value := range map2 {
		if _, ok := map1[map2Key]; !ok {
			resultMap[map2Key] = map2Value
		}
	}
	return resultMap
}
