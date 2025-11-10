# docker build -f src/Aspire.Dashboard/Dockerfile -t jessegador/loggle-aspire-dashboard:latest .

# Stage 1: restore and publish the Aspire dashboard with local customizations
ARG SDK_IMAGE=mcr.microsoft.com/dotnet/sdk:10.0
ARG RUNTIME_IMAGE=mcr.microsoft.com/dotnet/aspnet:9.0

FROM ${SDK_IMAGE} AS build
WORKDIR /workspace

# Bring in the repo-level build props, NuGet configuration, and engineering targets
COPY ["Directory.Build.props", "Directory.Build.targets", "Directory.Packages.props", "NuGet.config", "global.json", "./"]
COPY eng/ ./eng/

# Copy the source tree (contains Aspire.Dashboard and shared code)
COPY src/ ./src/

RUN dotnet restore src/Aspire.Dashboard/Aspire.Dashboard.csproj
RUN dotnet publish src/Aspire.Dashboard/Aspire.Dashboard.csproj \
    -c Release -o /app/publish /p:UseAppHost=false

# Stage 2: run on the ASP.NET runtime
FROM ${RUNTIME_IMAGE} AS final
WORKDIR /app

COPY --from=build /app/publish .

ENV ASPNETCORE_URLS=http://0.0.0.0:18888 \
    DOTNET_DASHBOARD_OTLP_ENDPOINT_URL=http://0.0.0.0:18889 \
    DOTNET_DASHBOARD_OTLP_HTTP_ENDPOINT_URL=http://0.0.0.0:18890

EXPOSE 18888 18889 18890

ENTRYPOINT ["dotnet", "Aspire.Dashboard.dll"]
