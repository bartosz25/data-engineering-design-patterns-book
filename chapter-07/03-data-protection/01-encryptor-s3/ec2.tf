resource "aws_instance" "s3_dedp_reader_instance" {
  ami           = "ami-0fb653ca2d3203ac1"
  instance_type = "t2.micro"
  iam_instance_profile = "${aws_iam_instance_profile.iam_key_reader_profile.name}"
}
