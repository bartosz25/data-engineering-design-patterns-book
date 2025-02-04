resource "aws_iam_role" "iam_key_reader" {
  name = "dedp_reader_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_instance_profile" "iam_key_reader_profile" {
  name = "dedp_reader_profile"
  role = "${aws_iam_role.iam_key_reader.name}"
}

resource "aws_iam_role_policy" "iam_key_reader_policy" {
  name = "dedp_reader_policy"
  role = "${aws_iam_role.iam_key_reader.id}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
        Effect = "Allow"
        Action = "s3:*"
        Resource : "*"
      }
    ]
  })
}