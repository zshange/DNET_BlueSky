# 安装docker
sudo apt update && sudo apt upgrade -y
sudo apt install -y curl git
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo apt install -y docker-compose-plugin
sudo mkdir -p /mydata
# 
sudo curl -L "https://github.com/docker/compose/releases/download/1.28.6/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose-1.28.6
sudo chmod +x /usr/local/bin/docker-compose-1.28.6
# 创建别名
echo 'alias docker-compose-1.28.6="/usr/local/bin/docker-compose-1.28.6"' >> ~/.bashrc
source ~/.bashrc

sudo cp ~/DNET_BlueSky/downloader/repo_downloader/docker-compose.yml  /mydata/docker-compose.yml
## 使用 postgres



## 使用mongodb
# # 创建目录 /mydata
# echo "创建目录 /mydata..."
# sudo mkdir -p /mydata

# # 执行 mkextrafs.pl 脚本
# echo "执行 mkextrafs.pl 脚本..."
# sudo /usr/local/etc/emulab/mkextrafs.pl /mydata

# # 创建 MongoDB 存储目录
# echo "创建 MongoDB 存储目录..."
# sudo mkdir -p /mydata/local/mongo
# sudo chown -R mongodb:mongodb /mydata/local/mongo

# # 启动 MongoDB 服务
# echo "启动 MongoDB 服务..."
# sudo systemctl start mongod
# sudo systemctl enable mongod

# # 修改 MongoDB 配置文件 /etc/mongod.conf
# echo "修改 MongoDB 配置文件 /etc/mongod.conf..."
# sudo sed -i 's|^  dbPath: .*|  dbPath: /mydata/local/mongo|' /etc/mongod.conf
# sudo sed -i 's|^  port: 27017|  port: 27018|' /etc/mongod.conf
# sudo sed -i 's|^  bindIp: 127.0.0.1|  bindIp: 0.0.0.0|' /etc/mongod.conf

# # 重启 MongoDB 服务以应用配置更改
# echo "重启 MongoDB 服务..."
# sudo systemctl restart mongodd