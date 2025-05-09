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


echo "脚本执行完毕!"

# 安装 jq
sudo apt-get install -y jq
# 安装 parallel
sudo apt-get install -y parallel
# 安装 mlr
sudo apt-get install -y miller

# 在conda环境中安装Go
echo "在conda环境中安装Go..."
conda install -y -c conda-forge go
# 验证Go安装
go version