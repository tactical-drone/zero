# unimatrix zero

A borg assimilated version of the iota reference implementation.

## Current status

A borg scout ship, a harmless vessel exploring the vast expanses of the tangle.

## Requirements

Requires dotnet core 2.2 SDK to be installed
- Windows
  - https://www.microsoft.com/net/download/dotnet-core/2.2
- Linux
  - Binaries
    - https://www.microsoft.com/net/download/thank-you/dotnet-sdk-2.2.100-preview3-linux-x64-binaries
  - apt-get (ubuntu 18.04)
    - `wget -q https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb`
    - `sudo dpkg -i packages-microsoft-prod.deb`
    - `sudo add-apt-repository universe`
    - `sudo apt-get update`
    - `sudo apt-get install dotnet-sdk-2.2`

- Windows
  - https://www.microsoft.com/net/download/thank-you/dotnet-sdk-2.2.100-preview3-windows-x64-binaries

## Building and running

- Windows & Linux both needs:
  - nodejs
  - npm
  - visual studio 2018 or `dotnet cmd line` 
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
   2. Switch to branch `dotnet-interop`
   3. **`~/../entangled$`** `bazel build //common:interop`
   4. Configure the location of entangled inside the file `fetch-interop-libs.sh` using the var `$ENTANGLED_DIR`
   5. Run **`~/../zero$`** `./fetch-interop-libs.sh`
   6. After this there should be a file called libinterop.so
3. Running 
   1. **`~/../zero$`** `dotnet run --project zero.sync`

### Web

In multiple consoles:

1. Do the steps above for the node then:
2. **`~/../zero$`** `dotnet restore`
3. **`~/../zero$`** `cd zero.web`
4. **`~/../zero.web$`** `npm install`
5. **`~/../zero.web$`** `cd ..`
   - Windows: (`START /B` does not work in powershell, just use two shells)
     1. `START /B dotnet run --project zero.api`
     2. `START /B dotnet run --project zero.web`
   - Linux: 
     1. **`~/../zero$`** `dotnet run --project zero.api & dotnet run --project zero.web`
8. Wait for the server to start, this can take a while sometimes while webpack does it's thing
7. Connect to __`https://localhost:5001`__
9. Enter the listening url for the local machine and press create, ie `tcp://192.168.1.2:15600`
10. Make sure remote iri node is peered up connecting back to your listening url in step 6



