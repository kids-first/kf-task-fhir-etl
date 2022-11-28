#
# Batch container instance security group resources
#
resource "aws_security_group_rule" "batch_http_egress" {
  type        = "egress"
  from_port   = 443 
  to_port     = 443 
  protocol    = "tcp"
  cidr_blocks = ["0.0.0.0/0"]

  security_group_id = aws_security_group.batch.id
}

resource "aws_security_group_rule" "batch_https_egress" {
  type        = "egress"
  from_port   = 5432 
  to_port     = 5432 
  protocol    = "tcp"
  cidr_blocks = ["0.0.0.0/0"]

  security_group_id = aws_security_group.batch.id
}

resource "aws_security_group_rule" "batch_fhir_egress" {
  type        = "egress"
  from_port   = 9000 
  to_port     = 9000
  protocol    = "tcp"
  cidr_blocks = ["0.0.0.0/0"]

  security_group_id = aws_security_group.batch.id
}
