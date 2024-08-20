variable "region" {
    description = "Region for project"
    default = "ca-central-1"
    type = string
}

# Prompted to set this on terraform plan/apply
variable "my_ip" {
    description = "My ip to view kafka web ui"
    type = string
}

variable "password_for_redshift_admin" {
    description = "Password for serverless redshift admin access"
    type = string
}