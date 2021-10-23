# Simple-Minio-Client

Just a simple Minio client.

Only supports fetching objects from a bucket so far. 

Created this since official Minio Client uses OkHTTP client which is [currently a bad option for GraalVM Native Image Isolates.](https://github.com/square/okhttp/issues/6702)

Probably better to use the official Minio Client for regular use cases.
