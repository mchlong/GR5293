# Final Project for GR5293, authors Wo Long, Victor Xiao

# LLM_RL_Project:
Integration of trading, LLM and RL

Data Sourse: https://drive.google.com/drive/folders/1UQ_ja6934zbwI87vKM1x5fMRApDkbvYh?usp=sharing

## Files for Data Pre-processing:
tr_data_pipeline.py

combine_parquets.py

Pre-process thomson reuters news corpus data in CBS grid, mask train set corpus.

## Files for Sentiment Signal Generation:
fingpt_sentiments_from_summarization.ipynb

news_summarization.ipynb

Summarize daily thomson reuters into digest by LLaMA 8b, then for sentiments generation with fingpt.

## Rule Based Strategy Backtest and FF Factor Decompostion:
rulebased.ipynb

## RL Train and Test
RL_portf_alloc_TD3_smaller_universe_demo.ipynb

RL_portf_alloc_TD3_final_result.ipynb

Interim and Final result of the RL model integrating Technical and Sentimental Signals


