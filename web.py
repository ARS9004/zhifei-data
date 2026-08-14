def test_oss_write():
    """极简测试：云端写入 OSS，验证格式是否对齐本地端"""
    import json
    import uuid
    from datetime import datetime
    
    test_session_id = str(uuid.uuid4())
    test_round_num = 999
    test_ts = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    
    # 构造和本地端完全一致的数据结构
    test_data = {
        "session_id": test_session_id,
        "round_num": test_round_num,
        "messages": {
            "messages": [
                {"role": "user", "content": f"测试消息 - 云端写入 {test_ts}"},
                {"role": "assistant", "content": f"测试回复 - 云端写入 {test_ts}"}
            ]
        },
        "ts": test_ts
    }
    
    # 写入 OSS
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "test_from_cloud.jsonl"  # 独立文件，不影响主文件
        content = json.dumps(test_data, ensure_ascii=False) + "\n"
        bucket.put_object(remote, content.encode('utf-8'))
        logger.info(f"✅ 测试数据已写入 OSS: {remote}")
        logger.info(f"   session_id: {test_session_id}")
        logger.info(f"   round_num: {test_round_num}")
        logger.info(f"   ts: {test_ts}")
        return True, test_session_id, test_ts
    except Exception as e:
        logger.error(f"❌ 测试写入失败: {e}")
        return False, None, None