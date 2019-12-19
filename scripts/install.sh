function reados()
{
    string=$(uname -rv)
    if [[ $string == *"Ubuntu"* || $string == *"Debian"* ]]; then
        echo 'debian'
    elif [[ $string == *"Darwin"* ]]; then
        echo 'darwin'
    fi
}

git submodule update --init --recursive

if [[ $(reados) == 'debian' ]]; then
    sudo apt install python3 python3-pip

    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" |sudo tee  /etc/apt/sources.list.d/pgdg.list
    sudo apt update
    sudo apt -y install postgresql-12 postgresql-client-12 postgresql-server-dev-12

    sudo -u postgres createuser -s $(whoami);
    eval "sudo -u postgres psql -c 'alter user \"$(whoami)\" with superuser;'"
elif [[ $(reados) == 'darwin' ]]; then
    brew install python3

    brew install postgresql@12
    brew services start postgresql
fi

cd hypopg
make
sudo make install
rm *.bc import/*.bc
cd ..

pip3 install -r requirements.txt
