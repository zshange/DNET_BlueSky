# 更新和升级系统
echo "更新和升级系统..."
sudo apt-get update -y
sudo apt-get upgrade -y

# 安装 MongoDB 工具和 MongoDB 服务
echo "安装 MongoDB 工具和 MongoDB 服务..."
sudo dpkg -i ~/BlueSky/script/Code4Livefeeds/mongodb-database-tools-ubuntu2004-x86_64-100.10.0.deb
sudo dpkg -i ~/BlueSky/script/Code4Livefeeds/mongodb-mongosh_2.3.2_amd64.deb
sudo dpkg -i ~/BlueSky/script/Code4Livefeeds/mongodb-org-server_5.0.30_amd64.deb

# 创建目录 /mydata
echo "创建目录 /mydata..."
sudo mkdir -p /mydata

# 执行 mkextrafs.pl 脚本
echo "执行 mkextrafs.pl 脚本..."
sudo /usr/local/etc/emulab/mkextrafs.pl /mydata

# 创建 MongoDB 存储目录
echo "创建 MongoDB 存储目录..."
sudo mkdir -p /mydata/local/mongo
sudo chown -R mongodb:mongodb /mydata/local/mongo

# 启动 MongoDB 服务
echo "启动 MongoDB 服务..."
sudo systemctl start mongod
sudo systemctl enable mongod

# 修改 MongoDB 配置文件 /etc/mongod.conf
echo "修改 MongoDB 配置文件 /etc/mongod.conf..."
sudo sed -i 's|^  dbPath: .*|  dbPath: /mydata/local/mongo|' /etc/mongod.conf
sudo sed -i 's|^  port: 27017|  port: 27018|' /etc/mongod.conf
sudo sed -i 's|^  bindIp: 127.0.0.1|  bindIp: 0.0.0.0|' /etc/mongod.conf

# 重启 MongoDB 服务以应用配置更改
echo "重启 MongoDB 服务..."
sudo systemctl restart mongod