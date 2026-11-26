#!/bin/sh
set -eu

if [ -f /tls/generated/ca.crt ]; then
    exit 0
fi

work=$(mktemp -d /tls/certs.XXXXXX)
trap 'rm -rf "$work"' EXIT
umask 077
openssl req -x509 -newkey rsa:2048 -noenc -days 825 \
    -subj "/CN=Stock Pipeline CA" -addext "basicConstraints=critical,CA:TRUE" \
    -keyout "$work/ca.key" -out "$work/ca.crt"

for service in postgres minio; do
    mkdir "$work/$service"
    openssl req -newkey rsa:2048 -noenc -subj "/CN=$service" \
        -keyout "$work/$service/private.key" -out "$work/$service/server.csr"
    printf 'subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' "$service" \
        > "$work/$service/extensions"
    openssl x509 -req -in "$work/$service/server.csr" -days 825 \
        -CA "$work/ca.crt" -CAkey "$work/ca.key" -CAcreateserial \
        -extfile "$work/$service/extensions" -out "$work/$service/public.crt"
    rm "$work/$service/server.csr" "$work/$service/extensions"
    chmod 755 "$work/$service"
    chmod 644 "$work/$service/public.crt"
done

chown 999:999 "$work/postgres/private.key"
rm "$work/ca.key" "$work/ca.srl"
chmod 755 "$work"
chmod 644 "$work/ca.crt"
mv "$work" /tls/generated
