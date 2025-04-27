"""
Analytics utilities for token data visualization
"""
import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)


def create_price_chart(
    mint_address: str, 
    price_data: List[Dict[str, Any]], 
    output_dir: str = "/tmp"
) -> Optional[str]:
    """
    Create price chart for a token
    
    Args:
        mint_address: Token mint address
        price_data: List of price data points
        output_dir: Directory to save the chart
        
    Returns:
        Path to the saved chart or None if failed
    """
    try:
        if not price_data:
            logger.warning(f"No price data for {mint_address}")
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(price_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Create figure
        plt.figure(figsize=(10, 6))
        plt.plot(df['timestamp'], df['price_usd'], 'b-', linewidth=2)
        
        # Add labels and title
        plt.title(f'Price History for {mint_address[:8]}...{mint_address[-4:]}', fontsize=14)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Price (USD)', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Format y-axis to show dollar amounts
        plt.gca().yaxis.set_major_formatter('${x:.6f}')
        
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45)
        
        # Tight layout to ensure everything fits
        plt.tight_layout()
        
        # Save the chart
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{mint_address}_price_chart.png")
        plt.savefig(output_path)
        plt.close()
        
        return output_path
    
    except Exception as e:
        logger.error(f"Error creating price chart: {e}")
        return None


def create_volume_chart(
    mint_address: str, 
    volume_data: List[Dict[str, Any]], 
    output_dir: str = "/tmp"
) -> Optional[str]:
    """
    Create volume chart for a token
    
    Args:
        mint_address: Token mint address
        volume_data: List of volume data points
        output_dir: Directory to save the chart
        
    Returns:
        Path to the saved chart or None if failed
    """
    try:
        if not volume_data:
            logger.warning(f"No volume data for {mint_address}")
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(volume_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Create figure
        plt.figure(figsize=(10, 6))
        plt.bar(df['timestamp'], df['volume_usd'], color='green', alpha=0.7)
        
        # Add labels and title
        plt.title(f'Trading Volume for {mint_address[:8]}...{mint_address[-4:]}', fontsize=14)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Volume (USD)', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Format y-axis to show dollar amounts
        plt.gca().yaxis.set_major_formatter('${x:,.0f}')
        
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45)
        
        # Tight layout to ensure everything fits
        plt.tight_layout()
        
        # Save the chart
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{mint_address}_volume_chart.png")
        plt.savefig(output_path)
        plt.close()
        
        return output_path
    
    except Exception as e:
        logger.error(f"Error creating volume chart: {e}")
        return None


def create_holders_pie_chart(
    mint_address: str, 
    holders_data: List[Dict[str, Any]], 
    output_dir: str = "/tmp"
) -> Optional[str]:
    """
    Create holders distribution pie chart for a token
    
    Args:
        mint_address: Token mint address
        holders_data: List of holder data
        output_dir: Directory to save the chart
        
    Returns:
        Path to the saved chart or None if failed
    """
    try:
        if not holders_data:
            logger.warning(f"No holders data for {mint_address}")
            return None
        
        # Prepare data
        top_holders = holders_data[:9]  # Top 9 holders
        
        # Calculate "Others" category
        others_percentage = 100 - sum(holder['percentage'] for holder in top_holders)
        if others_percentage > 0:
            top_holders.append({
                'wallet_address': 'Others',
                'percentage': others_percentage
            })
        
        # Extract data for pie chart
        labels = [f"{h['wallet_address'][:6]}...{h['wallet_address'][-4:]}" if h['wallet_address'] != 'Others' else 'Others' 
                 for h in top_holders]
        sizes = [h['percentage'] for h in top_holders]
        
        # Create figure
        plt.figure(figsize=(10, 8))
        plt.pie(sizes, labels=None, autopct='%1.1f%%', startangle=90, 
                shadow=False, explode=[0.05] * len(sizes))
        
        # Add title
        plt.title(f'Token Holder Distribution for {mint_address[:8]}...{mint_address[-4:]}', fontsize=14)
        
        # Add legend
        plt.legend(labels, loc="center left", bbox_to_anchor=(1, 0.5))
        
        # Equal aspect ratio ensures that pie is drawn as a circle
        plt.axis('equal')
        
        # Tight layout to ensure everything fits
        plt.tight_layout()
        
        # Save the chart
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{mint_address}_holders_chart.png")
        plt.savefig(output_path)
        plt.close()
        
        return output_path
    
    except Exception as e:
        logger.error(f"Error creating holders pie chart: {e}")
        return None


def generate_analytics_report(
    mint_address: str,
    token_data: Dict[str, Any],
    price_data: List[Dict[str, Any]],
    volume_data: List[Dict[str, Any]],
    holders_data: List[Dict[str, Any]],
    output_dir: str = "/tmp"
) -> Optional[str]:
    """
    Generate a comprehensive analytics report for a token
    
    Args:
        mint_address: Token mint address
        token_data: Token metadata
        price_data: List of price data points
        volume_data: List of volume data points
        holders_data: List of holder data
        output_dir: Directory to save the report
        
    Returns:
        Path to the saved report or None if failed
    """
    try:
        # Create charts
        price_chart = create_price_chart(mint_address, price_data, output_dir)
        volume_chart = create_volume_chart(mint_address, volume_data, output_dir)
        holders_chart = create_holders_pie_chart(mint_address, holders_data, output_dir)
        
        # Prepare report content
        token_name = token_data.get('name', 'Unknown')
        current_price = price_data[-1]['price_usd'] if price_data else 'N/A'
        current_fdv = price_data[-1]['fdv_usd'] if price_data else 'N/A'
        current_liquidity = price_data[-1]['liquidity_usd'] if price_data else 'N/A'
        
        # Calculate price change
        if len(price_data) >= 2:
            price_change_24h = ((price_data[-1]['price_usd'] / price_data[0]['price_usd']) - 1) * 100
            price_change_str = f"{price_change_24h:+.2f}%"
        else:
            price_change_str = 'N/A'
        
        # Calculate volume
        total_volume_24h = sum(v['volume_usd'] for v in volume_data[-24:]) if volume_data else 'N/A'
        
        # Generate report
        report_content = f"""# Analytics Report for {token_name}

## Token Information
- **Mint Address**: {mint_address}
- **Name**: {token_name}
- **Current Price**: ${current_price:.6f}
- **Fully Diluted Valuation**: ${current_fdv:,.0f}
- **Liquidity**: ${current_liquidity:,.0f}
- **24h Price Change**: {price_change_str}
- **24h Trading Volume**: ${total_volume_24h:,.0f}

## Price Chart
![Price Chart]({os.path.basename(price_chart) if price_chart else 'Not available'})

## Volume Chart
![Volume Chart]({os.path.basename(volume_chart) if volume_chart else 'Not available'})

## Holder Distribution
![Holder Distribution]({os.path.basename(holders_chart) if holders_chart else 'Not available'})

## Top Holders
| Rank | Wallet | Balance | Percentage |
|------|--------|---------|------------|
"""
        
        # Add top holders table
        for i, holder in enumerate(holders_data[:10]):
            wallet = holder['wallet_address']
            short_wallet = f"{wallet[:8]}...{wallet[-4:]}"
            balance = holder['balance']
            percentage = holder['percentage']
            
            report_content += f"| {i+1} | {short_wallet} | {balance:,.0f} | {percentage:.2f}% |\n"
        
        # Save report
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{mint_address}_analytics_report.md")
        
        with open(output_path, 'w') as f:
            f.write(report_content)
        
        return output_path
    
    except Exception as e:
        logger.error(f"Error generating analytics report: {e}")
        return None
