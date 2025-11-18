rm -rf history_price_old
mv history_price history_price_old
python async_history_price.py
python create_latest_aggregated_stock_info.py
rm -rf history_price_old

streamlit run streamlit_stock_filter.py