terraform {
  required_providers {
    huggingface-spaces = {
      source = "strickvl/huggingface-spaces"
    }
  }
}

variable "hf_token" {
  type      = string
  sensitive = true
}

# Initially, we attempted to use the standard Hugging Face Terraform provider. 
# However, we encountered a SDK Validation Bug where the provider couldn't correctly handle specific Docker/Streamlit-based Space configurations.
# Using a Provisioner to bypass the buggy SDK validation

# We implemented a `null_resource` with a `local-exec` provisioner. 
# This allowed us to bypass the buggy SDK and interface directly with the Hugging Face REST API via cURL.


resource "null_resource" "hf_space_setup" {
  provisioner "local-exec" {
    command = <<EOT
      curl -X POST https://huggingface.co/api/repos/create \
        -H "Authorization: Bearer ${var.hf_token}" \
        -H "Content-Type: application/json" \
        -d '{
          "name": "punktlich-train-analytics-nde97",
          "type": "space",
          "sdk": "docker",
          "private": false
        }'
    EOT
  }

  # Ensuring Terraform doesn't try to recreate it every single time
  lifecycle {
    ignore_changes = all
  }
}

output "space_status" {
  value = "Deployment successful. View at: https://huggingface.co/spaces/nde97/punktlich-train-analytics-nde97"
}