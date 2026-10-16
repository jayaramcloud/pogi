# S3 Vectors: a vector bucket with a policy, and a cosine index that keeps
# its "text" metadata out of filters. QueryVectors does exact k-NN.
resource "aws_s3vectors_vector_bucket" "embeddings" {
  vector_bucket_name = "tf-embeddings"
  force_destroy      = true
}

resource "aws_s3vectors_index" "docs" {
  vector_bucket_name = aws_s3vectors_vector_bucket.embeddings.vector_bucket_name
  index_name         = "docs"
  data_type          = "float32"
  dimension          = 384
  distance_metric    = "cosine"
  metadata_configuration { non_filterable_metadata_keys = ["text"] }
  tags = { team = "ml" }
}

resource "aws_s3vectors_vector_bucket_policy" "embeddings" {
  vector_bucket_arn = aws_s3vectors_vector_bucket.embeddings.vector_bucket_arn
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = "*", Action = "s3vectors:QueryVectors", Resource = aws_s3vectors_index.docs.index_arn }]
  })
}
