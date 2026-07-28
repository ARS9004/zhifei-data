#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import streamlit as st
import pymysql

st.set_page_config(page_title="RDS 连接测试", layout="centered")

st.title("🔌 RDS 连接测试 (云端)")

# 从 secrets 读取 RDS 配置
try:
    rds_host = st.secrets["connections"]["rds"]["host"]
    rds_port = st.secrets["connections"]["rds"]["port"]
    rds_user = st.secrets["connections"]["rds"]["username"]
    rds_password = st.secrets["connections"]["rds"]["password"]
    rds_database = st.secrets["connections"]["rds"]["database"]
    st.info(f"📋 配置已读取: {rds_host}:{rds_port}/{rds_database}")
except Exception as e:
    st.error(f"❌ 读取 Secrets 失败: {e}")
    st.stop()

if st.button("🔍 测试 RDS 连接"):
    with st.spinner("正在连接 RDS..."):
        try:
            conn = pymysql.connect(
                host=rds_host,
                port=rds_port,
                user=rds_user,
                password=rds_password,
                database=rds_database,
                charset='utf8mb4',
                connect_timeout=10
            )
            st.success("✅ 连接成功！")

            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM chat_memory")
                count = cursor.fetchone()[0]
                st.write(f"📊 chat_memory 表共有 **{count}** 条记录")

                cursor.execute("SELECT id, user_msg, assistant_msg, ts FROM chat_memory ORDER BY id DESC LIMIT 5")
                rows = cursor.fetchall()
                st.write("📝 最近 5 条记录：")
                for row in rows:
                    st.text(f"id={row[0]}, ts={row[3]}, user={row[1][:30]}...")

            conn.close()
            st.success("🎉 测试完成！RDS 可正常读取。")

        except pymysql.err.OperationalError as e:
            st.error(f"❌ 连接失败: {e}")
            st.warning("可能原因：RDS 白名单未放行 Streamlit Cloud 的 IP")
        except pymysql.err.ProgrammingError as e:
            st.error(f"❌ 查询失败: {e}")
            st.warning("可能原因：表名或字段名错误")
        except Exception as e:
            st.error(f"❌ 未知错误: {e}")