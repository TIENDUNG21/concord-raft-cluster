# Concord node image: build with the .NET 8 SDK, run on the ASP.NET Core runtime as a non-root user.
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY global.json Directory.Build.props Directory.Packages.props .editorconfig ./
COPY src/Concord.Core/Concord.Core.csproj src/Concord.Core/
COPY src/Concord.Kv/Concord.Kv.csproj src/Concord.Kv/
COPY src/Concord.Server/Concord.Server.csproj src/Concord.Server/
RUN dotnet restore src/Concord.Server/Concord.Server.csproj
COPY src/Concord.Core/ src/Concord.Core/
COPY src/Concord.Kv/ src/Concord.Kv/
COPY src/Concord.Server/ src/Concord.Server/
RUN dotnet publish src/Concord.Server/Concord.Server.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .
RUN mkdir /data && chown app:app /data
USER app
ENV ASPNETCORE_URLS=http://+:8080 \
    Concord__DataDirectory=/data
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["dotnet", "Concord.Server.dll"]
