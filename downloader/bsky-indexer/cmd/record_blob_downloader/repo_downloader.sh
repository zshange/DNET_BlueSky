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


# 创建目录 /mydata
echo "创建目录 /mydata..."
sudo mkdir -p /mydata

# 执行 mkextrafs.pl 脚本
echo "执行 mkextrafs.pl 脚本..."
sudo /usr/local/etc/emulab/mkextrafs.pl /mydata

sudo mkdir -p /mydata/csv
sudo mkdir -p /mydata/records
sudo mkdir -p /mydata/records_follow
sudo chmod -R 777 /mydata
chmod +x run.sh
