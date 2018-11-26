# unimatrix zero

A borg assimilated version of the iota reference implementation.

## Current status

A borg scout ship, a harmless vessel exploring the vast expanses of the tangle.

## Requirements

Requires dotnet core 2.2 SDK to be installed
- https://www.microsoft.com/net/download/dotnet-core/2.2
- Linux
  - https://www.microsoft.com/net/download/thank-you/dotnet-sdk-2.2.100-preview3-linux-x64-binaries
- Windows
  - https://www.microsoft.com/net/download/thank-you/dotnet-sdk-2.2.100-preview3-windows-x64-binaries

## Building and running

- nodejs
- npm
- visual studio 2018 or `dotnet cmd line`

### Console application
1. `dotnet restore`
2. `cd zero.sync`
3. `dotnet build`
4. `dotnet run`

### Web

In multiple consoles:

1. `dotnet restore`
2. `cd zero.web`
3. `npm install`
4. `cd ..`
   - Windows
     1. `START /B dotnet run --project zero.api`
     2. `START /B dotnet run --project zero.web`
   - Linux 
     1. `dotnet run --project zero.api & dotnet run --project zero.web`
7. Connect to `https://localhost:5001`
8. Create a dummy `username` `password`
9. Enter the listening url prepared for the local machine and press create, ie `tcp://192.168.1.2:15600`
10. Make sure remote iri node is peered up connecting back to your listening url in step 6



