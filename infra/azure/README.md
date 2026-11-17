# Azure IaaS handoff

No Azure resources were created. Deployment needs a subscription, region, VM size, SSH public key, and an operator-approved inbound CIDR; those choices have cost and security consequences and must not be fabricated.

The Bicep template defines an Ubuntu VM, an OS disk, an optional extra data disk, VNet/subnet, NIC, static public IP, managed identity and an NSG permitting SSH only from the supplied CIDR. It compiled successfully with Microsoft's Bicep compiler on 5 November 2026. No Azure what-if or deployment has run. Application ports remain private; use SSH forwarding for the academic demo.

After the student subscription is active, choose a region and confirm its actual VM/disk/public-IP costs before deploying. The default VM size is a configurable 4-vCPU/16-GB class, not a free-tier promise. Budget alerts notify but do not impose a hard cap. Do not run cloud resources continuously before the demo. Deallocating a VM stops compute billing but attached storage/public-IP resources may remain chargeable.

## Deployment procedure (pending account and cost approval)

1. Run `az deployment group what-if --resource-group <group> --template-file infra/azure/main.bicep --parameters adminUsername=<user> adminPublicKey="<public-key>" sshSourceCidr=<your-public-ip>/32`. Inspect the resource changes before deployment. The angle-bracket values are placeholders; supply actual values.
2. After approval, use the same arguments with `az deployment group create`.
3. SSH into the VM. Copy the source excluding `.git`, virtual environments, `.env`, datasets and transcripts. Never upload existing local secrets; generate separate cloud secrets on the VM.
4. Run `sudo bash infra/azure/bootstrap.sh` to install Docker from its official Ubuntu repository. An extra data disk is disabled by default to avoid unnecessary cost. If you selected a nonzero `dataDiskSizeGB`, inspect and mount that disk manually: the script intentionally does not format disks or move existing Docker data.
5. From the source directory, create local environment values using `python3 scripts/prepare_env.py`, then `sudo docker compose up -d --build`. The initial source build needs network access and time.
6. Forward ports over SSH, for example `ssh -L 8000:127.0.0.1:8000 -L 3000:127.0.0.1:3000 <user>@<vm-ip>`. Do not expose Kafka, PostgreSQL, MinIO or Spark directly to the public Internet.
7. Run the smoke, lake, recovery and monitoring checks in `docs/DEMO.md`. A successful Bicep compile proves template syntax, not available quota, regional SKU availability, cloud startup or application performance.

MinIO remains the S3 lake in this IaaS configuration. Azure Blob integration is not implemented; the original roadmap's suggestion that Blob automatically replaces MinIO is not true. An adapter or a Hadoop Azure connector would be required and needs separate verification.
