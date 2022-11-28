resource "aws_secretsmanager_secret" "fhir_password" {
  name = "${var.project}/${var.environment}/fhir_password"
  description = "${var.project} FHIR password"
}

resource "aws_secretsmanager_secret_version" "fhir_password" {
  secret_id     = aws_secretsmanager_secret.fhir_password.id
  secret_string = jsonencode(
	{
	}
  )
}

