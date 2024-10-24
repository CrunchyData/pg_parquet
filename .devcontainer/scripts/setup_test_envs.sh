# S3 tests
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=admin123
export AWS_REGION=us-east-1
export AWS_S3_TEST_BUCKET=testbucket
export MINIO_ROOT_USER=admin
export MINIO_ROOT_PASSWORD=admin123

# Azure Blob tests
export AZURE_TEST_CONTAINER_NAME=testcontainer
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"

# Other
export PG_PARQUET_TEST=true
export RUST_TEST_THREADS=1
