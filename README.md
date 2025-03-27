# tap-tapoptiply

This is a Singer tap for Optiply API, built with the Meltano Singer SDK.

## Installation

```bash
poetry install
```

## Configuration

Create a `config.json` file with your Optiply API credentials:

```json
{
    "apiCredentials": {
        "username": "your-username@optiply.nl",
        "password": "your-password",
        "account_id": 12345
    }
}
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's `.env` if the `--config=ENV` is provided, such that config values will be considered if a matching environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can run the tap using Meltano:

```bash
meltano elt tap-tapoptiply target-jsonl
```

Or directly using the tap:

```bash
poetry run tap-tapoptiply --config config.json
```

## Streams

The tap includes the following streams:

- `products`: Fetches product data from Optiply API
- `suppliers`: Fetches supplier data from Optiply API
- `supplier_products`: Fetches supplier product relationships
- `buy_orders`: Fetches purchase orders
- `buy_order_lines`: Fetches purchase order line items
- `sell_orders`: Fetches sales orders
- `sell_order_lines`: Fetches sales order line items

## Development

To set up the development environment:

1. Install Poetry
2. Run `poetry install`
3. Run tests with `poetry run pytest`

### Testing with Meltano

_**Note:** This tap will work in any Singer environment and does not require Meltano. Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-tapoptiply
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-tapoptiply --version
# OR run a test `elt` pipeline:
meltano run tap-tapoptiply target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to develop your own taps and targets.
