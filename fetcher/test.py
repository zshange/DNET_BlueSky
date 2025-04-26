import requests
from requests import Session
import time
from utils import read_repo_from_car



def car_list(car_path):
    with open(car_path, 'rb') as f:
        car_reader = f.read()
        r = read_repo_from_car(car_reader)
    return r


if __name__ == "__main__":
    # 定义 API 主机地址
    api_host = 'https://bsky.social'
    # Bluesky 认证信息
    identifier = "shange.bsky.social"  # 替换为你的 Bluesky 用户名
    password = "zqj20030403"  # 替换为你的 App Password
    # 首先获取认证令牌
    auth_response = requests.post(
        f"{api_host}/xrpc/com.atproto.server.createSession",
        json={"identifier": identifier, "password": password}
    )
    if auth_response.status_code != 200:
        print(f"认证失败：{auth_response.status_code}")
        print(f"错误信息：{auth_response.text}")
        exit(1)

    # 获取访问令牌
    access_jwt = auth_response.json()['accessJwt']

    # 定义请求的 Lexicon 接口路径 - 使用获取用户资料的API
    lexicon_path = '/xrpc/com.atproto.sync.listRepos'  
    repo_path = '/xrpc/com.atproto.sync.getRepo'
    get_repo_status_path = '/xrpc/com.atproto.sync.getRepoStatus'
    # 构建完整的 API URL
    url = f'{api_host}{lexicon_path}'
    repo_url = f'{api_host}{repo_path}'
    get_repo_status_url = f'{api_host}{get_repo_status_path}'
    # 添加认证头
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_jwt}',
        'Contact-Email': 'shange0403@gmail.com'  
    }

    # 必要的查询参数
    params_did = {
        'limit': 10
    }
    # 创建一个会话对象
    session = Session()

    # 发送 GET 请求
    try:
        print(f"正在请求URL: {url}")
        dids = session.get(url, headers=headers, params=params_did)
        time.sleep(1)
        
        # 检查响应状态码
        if dids.status_code == 200:
            data = dids.json()
            print("请求成功：")
            data_list = data['repos']
            cursor = data['cursor']
            print(f"获取到 {len(data_list)} 个仓库")
            print(f"第一个仓库的DID: {data_list[0]['did']}")
        else:
            print(f"第一个请求失败，状态码：{dids.status_code}")
            print(f"错误信息：{dids.text}")
            exit(1)
    except Exception as e:
        print(f"第一个请求发生错误：{str(e)}")
        exit(1)

    try:
        for did in data_list:
            params_repo = {
                'did': data_list[0]['did']
            }
            print(f"\n正在请求仓库URL: {repo_url}")
            print(f"使用参数: {params_repo}")
            repo = session.get(get_repo_status_url, headers=headers, params=params_repo)
            print(f"响应状态码: {repo.status_code}")
            print(type(repo))
            # print(f"响应内容: {repo.text[:200]}...")  # 只打印前200个字符
            
            if repo.status_code == 200:
                data_repo = repo.json()
                print("\n请求成功，仓库数据：")
                print(data_repo)
            else:
                print(f"\n第二个请求失败，状态码：{repo.status_code}")
                # print(f"错误信息：{repo.text}")
    except Exception as e:
        print(f"第二个请求发生错误：{str(e)}")
        # print(f"完整的响应内容：{repo.text}")