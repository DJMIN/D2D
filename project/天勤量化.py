# 引入TqSdk模块
from tqsdk import TqApi
# 创建api实例，设置web_gui=True生成图形化界面
api = TqApi(web_gui=True)
# 订阅 cu2002 合约的10秒线
klines = api.get_kline_serial("SHFE.cu2002", 10)
while True:
    # 通过wait_update刷新数据
    api.wait_update()