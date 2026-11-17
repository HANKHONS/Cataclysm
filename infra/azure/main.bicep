@description('Region for the IaaS virtual machine.')
param location string = resourceGroup().location
@description('Administrator username. Supply a non-default value at deployment.')
param adminUsername string
@secure()
param adminPublicKey string
param sshSourceCidr string
param vmSize string = 'Standard_D4s_v5'
param vmName string = 'cataclysm'
@description('Optional extra data disk; zero avoids provisioning an unused disk for a short demo.')
@minValue(0)
param dataDiskSizeGB int = 0

resource nsg 'Microsoft.Network/networkSecurityGroups@2023-11-01' = {
  name: 'cataclysm-nsg'
  location: location
  properties: { securityRules: [
    {
      name: 'ssh'
      properties: {
        priority: 100
        protocol: 'Tcp'
        access: 'Allow'
        direction: 'Inbound'
        sourceAddressPrefix: sshSourceCidr
        sourcePortRange: '*'
        destinationAddressPrefix: '*'
        destinationPortRange: '22'
      }
    }
  ] }
}

resource network 'Microsoft.Network/virtualNetworks@2023-11-01' = {
  name: '${vmName}-network'
  location: location
  properties: {
    addressSpace: { addressPrefixes: ['10.10.0.0/16'] }
    subnets: [{
      name: 'application'
      properties: {
        addressPrefix: '10.10.1.0/24'
        networkSecurityGroup: { id: nsg.id }
      }
    }]
  }
}

resource publicIp 'Microsoft.Network/publicIPAddresses@2023-11-01' = {
  name: '${vmName}-ip'
  location: location
  sku: { name: 'Standard' }
  properties: { publicIPAllocationMethod: 'Static' }
}

resource nic 'Microsoft.Network/networkInterfaces@2023-11-01' = {
  name: '${vmName}-nic'
  location: location
  properties: {
    ipConfigurations: [{
      name: 'primary'
      properties: {
        privateIPAllocationMethod: 'Dynamic'
        subnet: { id: '${network.id}/subnets/application' }
        publicIPAddress: { id: publicIp.id }
      }
    }]
  }
}

resource vm 'Microsoft.Compute/virtualMachines@2024-03-01' = {
  name: vmName
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    hardwareProfile: { vmSize: vmSize }
    storageProfile: {
      imageReference: {
        publisher: 'Canonical'
        offer: '0001-com-ubuntu-server-jammy'
        sku: '22_04-lts-gen2'
        version: 'latest'
      }
      osDisk: {
        createOption: 'FromImage'
        diskSizeGB: 64
        managedDisk: { storageAccountType: 'StandardSSD_LRS' }
      }
      dataDisks: dataDiskSizeGB > 0 ? [{
        lun: 0
        createOption: 'Empty'
        diskSizeGB: dataDiskSizeGB
        managedDisk: { storageAccountType: 'StandardSSD_LRS' }
      }] : []
    }
    osProfile: {
      computerName: vmName
      adminUsername: adminUsername
      linuxConfiguration: {
        disablePasswordAuthentication: true
        ssh: { publicKeys: [{ path: '/home/${adminUsername}/.ssh/authorized_keys', keyData: adminPublicKey }] }
      }
    }
    networkProfile: { networkInterfaces: [{ id: nic.id }] }
  }
}

output publicAddress string = publicIp.properties.ipAddress
output vmResourceId string = vm.id
