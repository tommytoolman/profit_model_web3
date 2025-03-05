from web3 import Web3
from chain_lib import w3_deejmon_http, w3_deejmon, chain_data, minimal_abi

W3 = w3_deejmon_http

usdc_address = chain_data['Ethereum']['usdc_address']
usdc_proxy = Web3.to_checksum_address("0x43506849d7c04f9138d1a2050bbf3a0c054402dd")
usdt_address = chain_data['Ethereum']['usdt_address']
weth_address = chain_data['Ethereum']['weth_address']
stETH_address = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"

weth_contract = W3.eth.contract(address=weth_address, abi=minimal_abi)
usdt_contract = W3.eth.contract(address=usdt_address, abi=minimal_abi)
usdc_contract = W3.eth.contract(address=usdc_address, abi=minimal_abi)
stETH_contract = W3.eth.contract(address=stETH_address, abi=minimal_abi)

miner_map = {
    "0x95222290DD7278Aa3Ddd389Cc1E1d165CC4BAfe5" : "beaverbuild",
    "0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97" : "Titan Builder",
    "0x1f9090aaE28b8a3dCeaDf281B0F12828e676c326" : "rsync-builder.eth",
    "0x77777A6C097a1cE65C61A96a49bd1100F660eC94" : "MEV Builder: 0x777...C",
    "0x965Df5Ff6116C395187E288e5C87fb96CfB8141c" : "bloXroute: Builder 1",
    "0x388C818CA8B9251b393131C08a736A67ccB19297" : "Lido: Execution Layer Rewards Vault",
    "0xdadB0d80178819F2319190D340ce9A924f783711" : "BuilderNet",
    "0x7e2a2FA2a064F693f0a55C5639476d913Ff12D05" : "MEV Builder: 0x7e2...D05",
    "0xd4E96eF8eee8678dBFf4d535E033Ed1a4F7605b7" : "Rocket Pool Smoothing Pool",
    "0xe688b84b23f322a994A53dbF8E15FA82CDB71127" : "Fee Recipient: 0xe68...127",
    "0xd11D7D2cb0aFF72A61Df37fD016EE1bd9F180633" : "MEV Builder: 0xd11...633",
    "0x9f4Cf329f4cF376B7ADED854D6054859dd102a2A" : "Fee Recipient: 0x9f4...a2A",
}

token_contracts = {
    'WETH': weth_contract,
    'USDC': usdc_contract,
    'USDT': usdt_contract,
    'stETH': stETH_contract,
    }