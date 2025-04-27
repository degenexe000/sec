"""
Advanced analytics utilities for Athena
"""
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import os
import json

from data.api_manager import APIManager
from data.transaction_detector import TransactionDetector

logger = logging.getLogger(__name__)


class AdvancedAnalytics:
    """Advanced analytics for token data"""
    
    def __init__(self, api_manager: APIManager, transaction_detector: TransactionDetector):
        """
        Initialize advanced analytics
        
        Args:
            api_manager: API manager for data access
            transaction_detector: Transaction detector for analysis
        """
        self.api_manager = api_manager
        self.transaction_detector = transaction_detector
        self.output_dir = "/tmp/athena_analytics"
        os.makedirs(self.output_dir, exist_ok=True)
    
    async def calculate_token_health_score(self, token_address: str) -> Dict[str, Any]:
        """
        Calculate a health score for a token based on various metrics
        
        Args:
            token_address: Token address
            
        Returns:
            Token health score data
        """
        try:
            # Get token data
            token_data = await self.api_manager.get_comprehensive_token_data(token_address)
            price_data = await self.api_manager.get_token_price_data(token_address)
            
            # Get transaction analysis
            tx_analysis = await self.transaction_detector.analyze_all_transactions(token_address)
            
            # Extract metrics
            liquidity_usd = price_data.get("liquidity_usd", 0)
            volume_24h = price_data.get("volume_24h", 0)
            price_change_24h = price_data.get("price_change_24h", 0)
            
            # Get DexScreener data for more metrics
            dexscreener_data = token_data.get("dexscreener", {})
            main_pair = dexscreener_data.get("main_pair", {})
            
            # Calculate liquidity to market cap ratio
            market_cap = 0
            if main_pair:
                fdv = float(main_pair.get("fdv", 0))
                market_cap = float(main_pair.get("mcap", fdv))
            
            liquidity_to_mcap = 0
            if market_cap > 0:
                liquidity_to_mcap = liquidity_usd / market_cap
            
            # Calculate volume to liquidity ratio
            volume_to_liquidity = 0
            if liquidity_usd > 0:
                volume_to_liquidity = volume_24h / liquidity_usd
            
            # Count special transactions
            team_tx_count = len(tx_analysis.get("team_transactions", []))
            insider_tx_count = len(tx_analysis.get("insider_transactions", []))
            sniper_tx_count = len(tx_analysis.get("sniper_transactions", []))
            total_tx_count = len(tx_analysis.get("all_transactions", []))
            
            # Calculate percentage of special transactions
            special_tx_percent = 0
            if total_tx_count > 0:
                special_tx_percent = (team_tx_count + insider_tx_count + sniper_tx_count) / total_tx_count * 100
            
            # Calculate health score components (0-100 scale)
            liquidity_score = min(100, max(0, liquidity_usd / 10000))  # $1M liquidity = 100 points
            volume_score = min(100, max(0, volume_to_liquidity * 100))  # 1x volume/liquidity = 100 points
            price_stability_score = min(100, max(0, 100 - abs(price_change_24h) * 2))  # Less volatility is better
            liquidity_mcap_score = min(100, max(0, liquidity_to_mcap * 500))  # 20% liquidity/mcap = 100 points
            transaction_score = min(100, max(0, 100 - special_tx_percent))  # Fewer special transactions is better
            
            # Calculate overall health score (weighted average)
            weights = {
                "liquidity": 0.3,
                "volume": 0.2,
                "price_stability": 0.15,
                "liquidity_mcap": 0.25,
                "transactions": 0.1
            }
            
            health_score = (
                liquidity_score * weights["liquidity"] +
                volume_score * weights["volume"] +
                price_stability_score * weights["price_stability"] +
                liquidity_mcap_score * weights["liquidity_mcap"] +
                transaction_score * weights["transactions"]
            )
            
            # Determine risk level
            risk_level = "High"
            if health_score >= 80:
                risk_level = "Low"
            elif health_score >= 50:
                risk_level = "Medium"
            
            return {
                "token_address": token_address,
                "health_score": health_score,
                "risk_level": risk_level,
                "components": {
                    "liquidity_score": liquidity_score,
                    "volume_score": volume_score,
                    "price_stability_score": price_stability_score,
                    "liquidity_mcap_score": liquidity_mcap_score,
                    "transaction_score": transaction_score
                },
                "metrics": {
                    "liquidity_usd": liquidity_usd,
                    "volume_24h": volume_24h,
                    "price_change_24h": price_change_24h,
                    "liquidity_to_mcap": liquidity_to_mcap,
                    "volume_to_liquidity": volume_to_liquidity,
                    "special_tx_percent": special_tx_percent
                }
            }
        
        except Exception as e:
            logger.error(f"Error calculating token health score: {e}")
            return {
                "token_address": token_address,
                "health_score": 0,
                "risk_level": "Unknown",
                "error": str(e)
            }
    
    async def detect_manipulation(self, token_address: str) -> Dict[str, Any]:
        """
        Detect potential price manipulation for a token
        
        Args:
            token_address: Token address
            
        Returns:
            Manipulation detection results
        """
        try:
            # Get token data
            price_data = await self.api_manager.get_token_price_data(token_address)
            token_data = await self.api_manager.get_comprehensive_token_data(token_address)
            
            # Get transaction analysis
            tx_analysis = await self.transaction_detector.analyze_all_transactions(token_address)
            
            # Get DexScreener data for price history
            dexscreener_data = token_data.get("dexscreener", {})
            candles = dexscreener_data.get("candles", {}).get("1h", [])
            
            # Check for wash trading (high volume but price doesn't move much)
            volume_24h = price_data.get("volume_24h", 0)
            liquidity_usd = price_data.get("liquidity_usd", 0)
            price_change_24h = abs(price_data.get("price_change_24h", 0))
            
            wash_trading_score = 0
            if liquidity_usd > 0 and price_change_24h < 5:  # Less than 5% price change
                volume_liquidity_ratio = volume_24h / liquidity_usd
                if volume_liquidity_ratio > 3:  # More than 3x volume/liquidity with little price change
                    wash_trading_score = min(100, volume_liquidity_ratio * 20)
            
            # Check for pump and dump patterns
            pump_dump_score = 0
            if candles and len(candles) > 24:
                # Convert candles to DataFrame
                df = pd.DataFrame(candles)
                if 'close' in df.columns and 'volume' in df.columns:
                    # Calculate price changes
                    df['price_change'] = df['close'].pct_change() * 100
                    
                    # Look for sudden price spikes followed by drops
                    max_spike = df['price_change'].max()
                    min_drop = df['price_change'].min()
                    
                    if max_spike > 20 and min_drop < -15:  # 20% spike and 15% drop
                        pump_dump_score = min(100, max_spike)
            
            # Check for team wallet selling
            team_selling_score = 0
            team_txs = tx_analysis.get("team_transactions", [])
            sell_count = sum(1 for tx in team_txs if tx.get("type") in ["sell"])
            
            if sell_count > 0:
                team_selling_score = min(100, sell_count * 20)
            
            # Calculate overall manipulation score
            manipulation_score = max(wash_trading_score, pump_dump_score, team_selling_score)
            
            # Determine manipulation level
            manipulation_level = "None"
            if manipulation_score >= 80:
                manipulation_level = "High"
            elif manipulation_score >= 50:
                manipulation_level = "Medium"
            elif manipulation_score >= 20:
                manipulation_level = "Low"
            
            return {
                "token_address": token_address,
                "manipulation_score": manipulation_score,
                "manipulation_level": manipulation_level,
                "components": {
                    "wash_trading_score": wash_trading_score,
                    "pump_dump_score": pump_dump_score,
                    "team_selling_score": team_selling_score
                },
                "metrics": {
                    "volume_24h": volume_24h,
                    "liquidity_usd": liquidity_usd,
                    "price_change_24h": price_change_24h,
                    "team_sell_count": sell_count
                }
            }
        
        except Exception as e:
            logger.error(f"Error detecting manipulation: {e}")
            return {
                "token_address": token_address,
                "manipulation_score": 0,
                "manipulation_level": "Unknown",
                "error": str(e)
            }
    
    async def predict_price_trend(self, token_address: str) -> Dict[str, Any]:
        """
        Predict price trend for a token based on historical data
        
        Args:
            token_address: Token address
            
        Returns:
            Price trend prediction results
        """
        try:
            # Get token data
            token_data = await self.api_manager.get_comprehensive_token_data(token_address)
            
            # Get DexScreener data for price history
            dexscreener_data = token_data.get("dexscreener", {})
            candles = dexscreener_data.get("candles", {}).get("1h", [])
            
            if not candles or len(candles) < 24:
                return {
                    "token_address": token_address,
                    "trend": "Unknown",
                    "confidence": 0,
                    "error": "Insufficient historical data"
                }
            
            # Convert candles to DataFrame
            df = pd.DataFrame(candles)
            if 'close' not in df.columns:
                return {
                    "token_address": token_address,
                    "trend": "Unknown",
                    "confidence": 0,
                    "error": "Invalid candle data format"
                }
            
            # Calculate simple moving averages
            df['sma_short'] = df['close'].rolling(window=6).mean()  # 6-hour SMA
            df['sma_long'] = df['close'].rolling(window=24).mean()  # 24-hour SMA
            
            # Drop NaN values
            df = df.dropna()
            
            if len(df) < 2:
                return {
                    "token_address": token_address,
                    "trend": "Unknown",
                    "confidence": 0,
                    "error": "Insufficient data after processing"
                }
            
            # Determine current trend
            current_short_sma = df['sma_short'].iloc[-1]
            current_long_sma = df['sma_long'].iloc[-1]
            prev_short_sma = df['sma_short'].iloc[-2]
            prev_long_sma = df['sma_long'].iloc[-2]
            
            # Calculate trend direction
            if current_short_sma > current_long_sma:
                if prev_short_sma <= prev_long_sma:
                    trend = "Bullish Crossover"  # Short SMA just crossed above long SMA
                    confidence = 80
                else:
                    trend = "Bullish"
                    # Calculate confidence based on the gap between SMAs
                    gap_percent = (current_short_sma - current_long_sma) / current_long_sma * 100
                    confidence = min(90, 50 + gap_percent * 5)
            elif current_short_sma < current_long_sma:
                if prev_short_sma >= prev_long_sma:
                    trend = "Bearish Crossover"  # Short SMA just crossed below long SMA
                    confidence = 80
                else:
                    trend = "Bearish"
                    # Calculate confidence based on the gap between SMAs
                    gap_percent = (current_long_sma - current_short_sma) / current_long_sma * 100
                    confidence = min(90, 50 + gap_percent * 5)
            else:
                trend = "Neutral"
                confidence = 50
            
            # Create a price chart with trend lines
            chart_path = self.create_trend_chart(token_address, df)
            
            return {
                "token_address": token_address,
                "trend": trend,
                "confidence": confidence,
                "chart_path": chart_path,
                "metrics": {
                    "current_price": df['close'].iloc[-1],
                    "short_sma": current_short_sma,
                    "long_sma": current_long_sma,
                    "price_change_1h": (df['close'].iloc[-1] / df['close'].iloc[-2] - 1) * 100 if len(df) >= 2 else 0,
                    "price_change_24h": (df['close'].iloc[-1] / df['close'].iloc[-24] - 1) * 100 if len(df) >= 24 else 0
                }
            }
        
        except Exception as e:
            logger.error(f"Error predicting price trend: {e}")
            return {
                "token_address": token_address,
                "trend": "Unknown",
                "confidence": 0,
                "error": str(e)
            }
    
    def create_trend_chart(self, token_address: str, df: pd.DataFrame) -> Optional[str]:
        """
        Create a price chart with trend lines
        
        Args:
            token_address: Token address
            df: DataFrame with price data
            
        Returns:
            Path to the saved chart or None if failed
        """
        try:
            plt.figure(figsize=(12, 6))
            
            # Plot price
            plt.plot(df.index, df['close'], label='Price', color='blue', alpha=0.7)
            
            # Plot SMAs
            plt.plot(df.index, df['sma_short'], label='6h SMA', color='green', linestyle='--')
            plt.plot(df.index, df['sma_long'], label='24h SMA', color='red', linestyle='-.')
            
            # Add labels and title
            plt.title(f'Price Trend Analysis for {token_address[:8]}...{token_address[-4:]}', fontsize=14)
            plt.xlabel('Time', fontsize=12)
            plt.ylabel('Price', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            # Tight layout
            plt.tight_layout()
            
            # Save chart
            output_path = os.path.join(self.output_dir, f"{token_address}_trend_chart.png")
            plt.savefig(output_path)
            plt.close()
            
            return output_path
        
        except Exception as e:
            logger.error(f"Error creating trend chart: {e}")
            return None
    
    async def generate_comprehensive_analysis(self, token_address: str) -> Dict[str, Any]:
        """
        Generate a comprehensive analysis for a token
        
        Args:
            token_address: Token address
            
        Returns:
            Comprehensive analysis results
        """
        try:
            # Run all analysis methods concurrently
            health_score_task = self.calculate_token_health_score(token_address)
            manipulation_task = self.detect_manipulation(token_address)
            trend_task = self.predict_price_trend(token_address)
            
            # Get token data
            token_data_task = self.api_manager.get_comprehensive_token_data(token_address)
            price_data_task = self.api_manager.get_token_price_data(token_address)
            tx_analysis_task = self.transaction_detector.analyze_all_transactions(token_address)
            
            # Wait for all tasks to complete
            health_score, manipulation, trend, token_data, price_data, tx_analysis = await asyncio.gather(
                health_score_task, manipulation_task, trend_task,
                token_data_task, price_data_task, tx_analysis_task
            )
            
            # Extract token metadata
            metadata = token_data.get("metadata", {})
            token_name = metadata.get("name", "Unknown Token")
            
            # Create analysis summary
            summary = (
                f"Token Health Score: {health_score.get('health_score', 0):.1f}/100 ({health_score.get('risk_level', 'Unknown')} Risk)\n"
                f"Manipulation Detection: {manipulation.get('manipulation_level', 'Unknown')}\n"
                f"Price Trend: {trend.get('trend', 'Unknown')} (Confidence: {trend.get('confidence', 0):.1f}%)\n"
                f"Current Price: ${price_data.get('price_usd', 0):.6f}\n"
                f"24h Change: {price_data.get('price_change_24h', 0):+.2f}%\n"
                f"Liquidity: ${price_data.get('liquidity_usd', 0):,.2f}\n"
                f"24h Volume: ${price_data.get('volume_24h', 0):,.2f}\n"
            )
            
            # Compile comprehensive analysis
            return {
                "token_address": token_address,
                "token_name": token_name,
                "summary": summary,
                "health_score": health_score,
                "manipulation": manipulation,
                "trend": trend,
                "price_data": price_data,
                "transaction_analysis": {
                    "team_tx_count": len(tx_analysis.get("team_transactions", [])),
                    "insider_tx_count": len(tx_analysis.get("insider_transactions", [])),
                    "sniper_tx_count": len(tx_analysis.get("sniper_transactions", [])),
                    "total_tx_count": len(tx_analysis.get("all_transactions", []))
                },
                "generated_at": datetime.utcnow().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error generating comprehensive analysis: {e}")
            return {
                "token_address": token_address,
                "error": str(e)
            }
