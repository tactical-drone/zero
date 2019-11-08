# unimatrix zero

A borg assimilated version of the iota reference implementation.

## Current status

A borg scout ship, a harmless vessel exploring the vast expanses of the tangle:

- *zero.tangle*: The iota tangle node implementation
- *zero.sync*: Listens for IRI connections and displays non zero transactions on screen. 
  - If a cassandra host is provided data will be loaded. 
  - If a redis host is provided incoming TXs will be dup checked
- *zero.api*: Services *zero.web*
- *zero.web*: A basic web framework POC that displays a transactions live stream that can be filtered on TAG

**Configuration is non existant** so to listener IP, cassandra IP and redis IP needs to be manually edited in the code and recompiled.

*zero.sync* **listening port must match** iri listening port. This is a current limitation

**Only TCP connections are supported** at the moment. *dotnet core* UDP has some strange issue with CRC offloading and fragmented UDP packets. UDP connections might work in linux.

The recent upgrade to *dotnet 3 preview 2* **broked the API listener**. It says it is listening but you cant connect to it. Still needs investigation, but this means *zero.web* will not run at the moment.

Basically all that is working right now is *zero.sync*.

Optionally start cassandra and redis hosts and you will gain data loading that you can view with [Datastax DevCenter](https://academy.datastax.com/all-downloads)

- `docker run -p 6379:6379 --name redis -d redis`
- `docker run --name cassandra -d cassandra -p 9042:9042 `

## Requirements

Requires **3.0.100-preview5** to be installed
- Windows
  - https://dotnet.microsoft.com/download/dotnet-core/3.0
- Linux
  - Binaries
    - https://dotnet.microsoft.com/download/thank-you/dotnet-sdk-3.0.100-preview5-linux-x64-binaries
    - Extract the tarball to some folder and set it in path
  - apt-get (ubuntu 18.04) (not supported yet for sdk-3.0, use above manual binary extraction)
    - `wget -q https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb`
    - `sudo dpkg -i packages-microsoft-prod.deb`
    - `sudo add-apt-repository universe`
    - `sudo apt-get update`
    - `sudo apt-get install dotnet-sdk-3.0`

- Windows
  - https://dotnet.microsoft.com/download/thank-you/dotnet-sdk-3.0.100-preview5-windows-x64-installer

## Building and running

- Windows & Linux both needs:
  - nodejs
  - npm
  - **visual studio 2019** or `dotnet cmd line`
	- enable dotnet3 support: tools -> options -> .Net Core -> Check: **Use preview of Dotnet Core SDK**
- Linux also needs:
  - [bazel](https://bazel.build/)
  - [Entangled - branch dotnet-interop](https://gitlab.com/unimatrix-one/entangled/tree/dotnet-interop)

### Just the node

Most commands must be executed from the root folder **`~/../zero$`**:

1. Build dotnet apps
   1. **`~/../zero$`** `dotnet restore`
   2. **`~/../zero$`** `dotnet build`
2. Entangled
   1. Clone the repo
   2. **`~/../zero$`** `git submodule init`
   3. **`~/../zero$`** `git submodule update`
   4. Switch to branch `dotnet-interop`
   5. **`~/../entangled$`** `bazel build //common:interop`
   6. Configure the location of entangled inside the file `fetch-interop-libs.sh` using the var `$ENTANGLED_DIR`
   7. Run **`~/../zero$`** `./fetch-interop-libs.sh`
   8. After this there should be a file called libinterop.so
3. Running (**TCP connects supported only**, UDP is not supported because of crc offload is broken on windows)
   1. Edit **`~/../zero/zero.sync/Program.cs`** and insert appropriate listen IP (config is on it's way)
      - **Warning**: Make sure *zero* listening port and *iri* listening port matches. This is a limitation currently that will  be fixed soon
   2. **`~/../zero$`** `dotnet run --project zero.sync`

### Web (currently not working on dotnet3 beta, the API listener is not working)

In multiple consoles:

1. Do the steps above for the node then:
2. **`~/../zero$`** `dotnet restore`
3. **`~/../zero$`** `cd zero.web`
4. **`~/../zero.web$`** `npm install`
5. **`~/../zero.web$`** `dotnet dev-certs https`
6. **`~/../zero.web$`** `cd ..`
   - Windows: (`START /B` does not work in powershell, just use two shells)
     1. `START /B dotnet run --project zero.api`
     2. `START /B dotnet run --project zero.web`
   - Linux: 
     1. **`~/../zero$`** `dotnet run --project zero.api & dotnet run --project zero.web`
7. Wait for the server to start, this can take a while sometimes while webpack does it's thing
8. Connect to __`https://localhost:5001`__
9. Enter the listening url for the local machine and press create, ie `tcp://192.168.1.2:15600`
10. Make sure remote iri node is peered up connecting back to your listening url in step 6



