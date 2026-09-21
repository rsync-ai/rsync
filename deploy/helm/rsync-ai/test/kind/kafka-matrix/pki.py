"""Throwaway PKI for the Kafka security matrix.

    python pki.py OUT_DIR BROKER_HOST

Runs in the test OIDC image (it already carries `cryptography`), so the matrix
needs no host openssl -- macOS ships LibreSSL, whose flags differ.

Writes, all 0644 (read by containers running as other uids):
  ca.crt                         trust anchor for the broker and every client
  broker-keystore.pem            broker leaf + CA + PKCS#8 key (a JVM PEM keystore)
  client.crt / client.key        mTLS pair, key PKCS#8 -- the two-path shape
  client.pem                     the same pair as ONE file -- the JVM keystore shape
  client-pkcs1.key               the same key as PKCS#1 ("BEGIN RSA PRIVATE KEY"),
                                 which a JVM PEM keystore cannot load
  rogue-ca.crt                   a CA the broker's chain does not lead to
  rogue-client.{crt,key,pem}     a client pair signed by the rogue CA

The CA private keys are never written: nothing needs them after signing.
"""
import datetime
import os
import sys

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

NOW = datetime.datetime.now(datetime.timezone.utc)


def key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def name(cn):
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def cert(subject_cn, subject_key, issuer_cn, issuer_key, *, ca=False, eku=(), sans=()):
    b = (x509.CertificateBuilder()
         .subject_name(name(subject_cn))
         .issuer_name(name(issuer_cn))
         .public_key(subject_key.public_key())
         .serial_number(x509.random_serial_number())
         .not_valid_before(NOW - datetime.timedelta(minutes=5))
         .not_valid_after(NOW + datetime.timedelta(days=2))
         .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True))
    if ca:
        b = b.add_extension(x509.KeyUsage(
            digital_signature=True, key_cert_sign=True, crl_sign=True,
            content_commitment=False, key_encipherment=False, data_encipherment=False,
            key_agreement=False, encipher_only=False, decipher_only=False), critical=True)
    if eku:
        b = b.add_extension(x509.ExtendedKeyUsage(list(eku)), critical=False)
    if sans:
        b = b.add_extension(x509.SubjectAlternativeName([x509.DNSName(s) for s in sans]),
                            critical=False)
    return b.sign(issuer_key, hashes.SHA256())


def pem_cert(c):
    return c.public_bytes(serialization.Encoding.PEM)


def pem_key(k, fmt=serialization.PrivateFormat.PKCS8):
    return k.private_bytes(serialization.Encoding.PEM, fmt, serialization.NoEncryption())


def main(out, broker_host):
    os.makedirs(out, exist_ok=True)
    files = {}

    ca_key = key()
    ca = cert("kmatrix-ca", ca_key, "kmatrix-ca", ca_key, ca=True)
    rogue_ca_key = key()
    rogue_ca = cert("kmatrix-rogue-ca", rogue_ca_key, "kmatrix-rogue-ca", rogue_ca_key, ca=True)

    broker_key = key()
    broker = cert(broker_host, broker_key, "kmatrix-ca", ca_key,
                  eku=(ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH),
                  sans=(broker_host, "localhost"))
    client_key = key()
    client = cert("kmatrix-client", client_key, "kmatrix-ca", ca_key,
                  eku=(ExtendedKeyUsageOID.CLIENT_AUTH,))
    rogue_key = key()
    rogue = cert("kmatrix-rogue-client", rogue_key, "kmatrix-rogue-ca", rogue_ca_key,
                 eku=(ExtendedKeyUsageOID.CLIENT_AUTH,))

    files["ca.crt"] = pem_cert(ca)
    files["broker-keystore.pem"] = pem_cert(broker) + pem_cert(ca) + pem_key(broker_key)
    files["client.crt"] = pem_cert(client)
    files["client.key"] = pem_key(client_key)
    files["client.pem"] = pem_cert(client) + pem_key(client_key)
    files["client-pkcs1.key"] = pem_key(client_key, serialization.PrivateFormat.TraditionalOpenSSL)
    files["rogue-ca.crt"] = pem_cert(rogue_ca)
    files["rogue-client.crt"] = pem_cert(rogue)
    files["rogue-client.key"] = pem_key(rogue_key)
    files["rogue-client.pem"] = pem_cert(rogue) + pem_key(rogue_key)

    # The rows that must fail are only meaningful if the material is what its
    # name says. Check the two properties they depend on.
    assert files["client.key"].startswith(b"-----BEGIN PRIVATE KEY"), "client.key is not PKCS#8"
    assert files["client-pkcs1.key"].startswith(b"-----BEGIN RSA PRIVATE KEY"), "PKCS#1 variant is not PKCS#1"
    client.verify_directly_issued_by(ca)
    try:
        rogue.verify_directly_issued_by(ca)
    except Exception:  # noqa: BLE001 -- any verification failure is the point
        pass
    else:
        raise AssertionError("rogue client cert verifies against the real CA")

    for fname, data in files.items():
        path = os.path.join(out, fname)
        with open(path, "wb") as f:
            f.write(data)
        os.chmod(path, 0o644)
    print(f"pki: wrote {len(files)} files for broker host {broker_host}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
