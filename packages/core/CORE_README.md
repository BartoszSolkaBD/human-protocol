# Core Contracts Guide 

## Overview 

This document contains information about scripts that helps with the deployment, initialization and interaction with
Core Contracts for Human Protocol using [Foundry](https://book.getfoundry.sh/)

## Installation 

### Install Foundry

Foundry is required for compiling, testing, and deploying the smart contracts. To install Foundry, run the following command in your terminal:

```bash
curl -L https://foundry.paradigm.xyz | bash
source $HOME/.profile  # or .bash_profile, .bashrc, .zshrc etc. depending on your shell
foundryup
```

## Upgrading Contract Using Proxies  
-Create an .env file based on .env.example 
- Fill out these variables (**HMT_ADDRESS, STAKING_PROXY, ESCROW_FACTORY_PROXY, REWARD_POOL_PROXY**) with their respective values. 
- Then, run : ```forge script script/UpgradeProxies.s.sol:UpgradeProxiesScript --rpc-url $NETWORK --broadcast --verify --legacy```
- Make sure the PRIVATE_KEY of the address who deployed the proxies is the one running this command. 

## Testing Contracts 

To test contracts using Foundry run : 

- ```forge test --match-path ./test/<FILE_NAME>``` to run all tests for a specific file. 
- ```forge test --match-path ./test/<FILE_NALE> --match-test "<TEST_NAME"``` to run a specific test for a specific file. 

## Deploying to Mumbai (test)

- Deploy Core : 

- ```forge script script/DeployCore.s.sol:DeployCore --rpc-url polygonMumbai --broadcast --verify --legacy```

- Deploy Proxies : 

- ```forge script script/DeployProxies.s.sol:DeployProxies --rpc-url polygonMumbai --broadcast --verify --legacy```

- Upgrade Proxies : (add **HMT_ADDRESS, STAKING_PROXY, ESCROW_FACTORY_PROXY, REWARD_POOL_PROXY** in .env ) 

- ```forge script script/UpgradeProxies.s.sol:UpgradeProxiesScript --rpc-url polygonMumbai --broadcast --verify --legacy```





