# LLM-Powered Trading Strategy with Reinforcement Learning

*Authors: Wo Long, Victor Xiao (GR5293 Final Project)*

## 📋 Overview

This project integrates Large Language Models (LLMs), Reinforcement Learning (RL), and financial trading strategies to create an advanced portfolio allocation system. We use Thomson Reuters news data to generate sentiment signals, which are then combined with technical analysis to inform RL-based trading decisions.

## 🚀 Quick Start

1. Clone this repository
2. Download the dataset from [Google Drive](https://drive.google.com/drive/folders/1UQ_ja6934zbwI87vKM1x5fMRApDkbvYh?usp=sharing)
3. Run the data preprocessing scripts
4. Explore the notebooks based on your area of interest

## 📁 Project Structure

### Data Preprocessing
- `tr_data_pipeline.py` - Processes Thomson Reuters news corpus data
- `combine_parquets.py` - Combines and organizes parquet files
- `price_pipeline_modified.ipynb` - Prepares price data for analysis

### Sentiment Analysis
- `news_summarization.ipynb` - Summarizes daily Thomson Reuters news using LLaMA 8b
- `fingpt_sentiments_from_summarization.ipynb` - Generates sentiment signals using FinGPT
- `sentiment_analysis.ipynb` & `sentiment_analysis_v2_backup.ipynb` - Alternative sentiment analysis approaches

### Trading Strategies
- `rulebased.ipynb` - Rule-based strategy backtesting and Fama-French factor decomposition

### Reinforcement Learning
- `RL_portf_alloc_TD3_smaller_universe_demo.ipynb` - TD3 RL model implementation demo
- `RL_portf_alloc_TD3_final_result.ipynb` - Final results integrating technical and sentiment signals
- `td3_retrained_model.zip` - Saved model weights

## 🔍 Implementation Details

Our approach follows these key steps:
1. Process financial news data from Thomson Reuters
2. Generate daily news summaries using LLaMA 8b
3. Extract sentiment signals using FinGPT
4. Combine sentiment signals with technical indicators
5. Train a TD3 reinforcement learning agent for portfolio allocation
6. Evaluate performance against benchmark strategies

## 📊 Results

For detailed results and performance metrics, see the `RL_portf_alloc_TD3_final_result.ipynb` notebook and the project report in the `Report` folder.

## 📚 Citation

If you use this code or methodology in your research, please cite our project.


