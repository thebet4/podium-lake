from podium_lake.config import settings


def main() -> None:
    print(f"API URL: {settings.api_url}")
    print(f"Debug:   {settings.debug_mode}")

    if settings.debug_mode:
        print("Running in debug mode")


if __name__ == "__main__":
    main()
