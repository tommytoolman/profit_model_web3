"""
Wallet Grouping Utility for MEV Analysis

This script helps detect and group related wallets based on transaction patterns.
It can be used to identify wallets likely belonging to the same entity.
"""

import pandas as pd
import numpy as np
import networkx as nx
from web3 import Web3
from collections import defaultdict
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv
from argparse import ArgumentParser

from lib_etherscan_funcs import get_address_tx_hashes_and_blocks
from chain_lib import w3_deejmon_http, chain_data
from metadata import token_contracts

load_dotenv()

def detect_related_wallets(address_list, start_block, min_tx_threshold=3, batch_size=200):
    """
    Detect potentially related wallets based on transaction patterns
    
    Args:
        address_list: List of addresses to analyze
        start_block: Block number to start analysis from
        min_tx_threshold: Minimum number of transactions between addresses to consider them related
        batch_size: Batch size for API requests
    
    Returns:
        dict: Grouped addresses by entity
    """
    # Normalize addresses
    address_list = [Web3.to_checksum_address(addr.lower()) for addr in address_list]
    
    # Create a graph to represent wallet relationships
    G = nx.Graph()
    
    # Add all addresses as nodes
    for addr in address_list:
        G.add_node(addr, type='wallet')
    
    # Process each address
    for idx, addr in enumerate(address_list):
        print(f"Processing address {idx+1}/{len(address_list)}: {addr}")
        
        # Get transaction history for this address
        tx_list = get_address_tx_hashes_and_blocks(addr, start_block)
        
        if not tx_list:
            print(f"No transactions found for {addr}")
            continue
            
        # Create a DataFrame from transaction list
        df = pd.DataFrame(tx_list)
        
        # Map transaction counterparties
        tx_counterparties = defaultdict(int)
        
        # For each transaction, check if counterparty is in our address list
        for _, row in df.iterrows():
            # Transactions from this address to others in our list
            if row['from'].lower() == addr.lower() and row['to'].lower() in [a.lower() for a in address_list]:
                tx_counterparties[Web3.to_checksum_address(row['to'].lower())] += 1
                
            # Transactions to this address from others in our list
            if row['to'].lower() == addr.lower() and row['from'].lower() in [a.lower() for a in address_list]:
                tx_counterparties[Web3.to_checksum_address(row['from'].lower())] += 1
        
        # Add edges for relationships that meet the threshold
        for counterparty, count in tx_counterparties.items():
            if count >= min_tx_threshold:
                G.add_edge(addr, counterparty, weight=count)
                print(f"  Found relationship: {addr} <-> {counterparty} ({count} transactions)")
    
    # Find connected components (groups of related addresses)
    components = list(nx.connected_components(G))
    
    # Create entity grouping
    entity_groups = {}
    for i, component in enumerate(components):
        if len(component) > 1:  # Only include groups with multiple addresses
            entity_groups[f"Entity{i+1}"] = list(component)
    
    # Add addresses that didn't match any group as individual entities
    for addr in address_list:
        if not any(addr in group for group in entity_groups.values()):
            entity_groups[f"Individual_{addr[:8]}"] = [addr]
    
    # Visualize the graph
    visualize_wallet_network(G, entity_groups)
    
    return entity_groups

def visualize_wallet_network(G, entity_groups):
    """
    Create a visualization of wallet relationships
    
    Args:
        G: NetworkX graph
        entity_groups: Dictionary of entity groups
    """
    plt.figure(figsize=(12, 10))
    
    # Create color map for entities
    colors = plt.cm.tab10(np.linspace(0, 1, len(entity_groups)))
    color_map = {}
    
    for i, (entity, addrs) in enumerate(entity_groups.items()):
        for addr in addrs:
            color_map[addr] = colors[i]
    
    # Set node colors
    node_colors = [color_map.get(node, 'gray') for node in G.nodes()]
    
    # Calculate positions
    pos = nx.spring_layout(G, k=0.3, iterations=50)
    
    # Draw the network
    nx.draw_networkx_nodes(G, pos, node_size=700, node_color=node_colors, alpha=0.8)
    nx.draw_networkx_edges(G, pos, width=[G[u][v].get('weight', 1)/5 for u, v in G.edges()], alpha=0.5)
    
    # Add labels
    labels = {addr: addr[:6] + "..." for addr in G.nodes()}
    nx.draw_networkx_labels(G, pos, labels=labels, font_size=10)
    
    # Add a legend
    legend_elements = [plt.Line2D([0], [0], marker='o', color='w', 
                               markerfacecolor=colors[i], markersize=10, label=entity) 
                    for i, entity in enumerate(entity_groups.keys())]
    
    plt.legend(handles=legend_elements, loc='best')
    plt.title("Wallet Relationship Network")
    plt.axis('off')
    
    # Save and show
    plt.savefig("wallet_network.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Network visualization saved to wallet_network.png")

def save_entity_groups(entity_groups, output_file="entity_groups.py"):
    """
    Save entity groups to a Python file that can be imported
    
    Args:
        entity_groups: Dictionary of entity groups
        output_file: Output Python file
    """
    with open(output_file, "w") as f:
        f.write("# Auto-generated entity groups for MEV analysis\n\n")
        f.write("entity_groups = {\n")
        
        for entity, addresses in entity_groups.items():
            f.write(f"    \"{entity}\": [\n")
            for addr in addresses:
                f.write(f"        \"{addr}\",\n")
            f.write("    ],\n")
        
        f.write("}\n\n")
        
        # Add reverse mapping
        f.write("# Reverse mapping for quick lookup\n")
        f.write("address_to_entity = {}\n")
        f.write("for entity, addresses in entity_groups.items():\n")
        f.write("    for addr in addresses:\n")
        f.write("        address_to_entity[addr.lower()] = entity\n")
    
    print(f"Entity groups saved to {output_file}")

def main():
    parser = ArgumentParser(description="Detect and group related wallets for MEV analysis")
    parser.add_argument("--addresses", type=str, help="Comma-separated list of addresses (overrides .env)")
    parser.add_argument("--start_block", type=int, default=19000000, help="Starting block for analysis")
    parser.add_argument("--min_tx", type=int, default=3, help="Minimum transactions to consider addresses related")
    parser.add_argument("--output", type=str, default="entity_groups.py", help="Output file for entity groups")
    
    args = parser.parse_args()
    
    # Get addresses
    if args.addresses:
        addresses = [addr.strip() for addr in args.addresses.split(',')]
    else:
        addresses = os.environ.get("address_list", "").split(",")
    
    if not addresses or not addresses[0]:
        print("No addresses provided. Use --addresses or set address_list in .env")
        return
    
    print(f"Analyzing {len(addresses)} addresses starting from block {args.start_block}")
    
    # Detect related wallets
    entity_groups = detect_related_wallets(
        addresses, 
        args.start_block, 
        min_tx_threshold=args.min_tx
    )
    
    # Print results
    print("\nDetected entity groups:")
    for entity, addrs in entity_groups.items():
        addr_list = ", ".join([a[:8] + "..." for a in addrs])
        print(f"  {entity}: {addr_list} ({len(addrs)} addresses)")
    
    # Save results
    save_entity_groups(entity_groups, args.output)

if __name__ == "__main__":
    main()