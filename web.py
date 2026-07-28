import streamlit as st
import pymysql
import sys

st.set_page_config(page_title="智飞投研 · 测试版", layout="centered")

st.title("✅ 智飞投研 · 基础版")
st.write("应用已成功启动，当前没有任何外部依赖。")
st.write("接下来可以逐步添加 RDS 和百炼功能。")

col1, col2 = st.columns(2)
with col1:
    if st.button("点击测试"):
        st.info("按钮响应正常，Streamlit 工作正常。")

with col2:
    if st.button("🔌 测试 RDS 连接"):
        st.write("正在测试 RDS 连接...")
        
        try:
            # 从 secrets 读取 RDS 配置
            host = st.secrets["connections"]["rds"]["host"]
            port = st.secrets["connections"]["rds"]["port"]
            user = st.secrets["connections"]["rds"]["username"]
            password = st.secrets["connections"]["rds"]["password"]
            database = st.secrets["connections"]["rds"]["database"]
            st.write(f"📋 配置已读取: {host}:{port}/{database}")
            
            # 尝试连接
            conn = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                charset='utf8mb4',
                connect_timeout=10
            )
            st.success("✅ RDS 连接成功！")
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM chat_memory")
                count = cursor.fetchone()[0]
                st.write(f"📊 chat_memory 表共有 **{count}** 条记录")
                
                cursor.execute("SELECT id, user_msg FROM chat_memory ORDER BY id DESC LIMIT 3")
                rows = cursor.fetchall()
                st.write("📝 最近 3 条记录：")
                for row in rows:
                    st.write(f"  id={row[0]}, msg={row[1][:30]}...")
            
            conn.close()
            st.success("🎉 RDS 连接和读取全部正常！")
            
        except Exception as e:
            st.error(f"❌ RDS 连接或查询失败: {e}")