# 更新和升级系统
echo "更新和升级系统..."
sudo apt-get update -y
sudo apt-get upgrade -y

# 安装 Miniconda3
echo "安装 Miniconda3..."
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/miniconda3

# 自动执行 conda 初始化
echo "初始化 Miniconda3..."
export PATH="$HOME/miniconda3/bin:$PATH"
conda init
source ~/.bashrc


# 通过 conda 安装 Go 环境
echo "安装 Go 环境..."
conda install -c conda-forge go -y

# 验证 Go 安装
echo "验证 Go 安装..."
go version




# 创建目录 /mydata
echo "创建目录 /mydata..."
sudo mkdir -p /mydata

# 执行 mkextrafs.pl 脚本
echo "执行 mkextrafs.pl 脚本..."
sudo /usr/local/etc/emulab/mkextrafs.pl /mydata

# 安装docker和docker-compose
# Docker version 28.1.1, build 4eba377
# Docker Compose version v2.35.1

echo "安装 Docker..."
# 卸载旧版本
sudo apt-get remove docker docker-engine docker.io containerd runc
# 设置存储库
sudo apt-get update
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

# 添加 Docker 官方 GPG 密钥
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# 设置 Docker 仓库
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 安装指定版本的 Docker Engine
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io

# 安装指定版本的 Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.35.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# 验证安装
docker --version
docker-compose --version

