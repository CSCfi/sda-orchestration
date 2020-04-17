import jinja2
import aiohttp_jinja2
from aiohttp import web
from pathlib import Path
import os
import sys
from .utils.logger import LOG
from .utils.db_ops import retrieve_file2dataset
from typing import Dict, Any

router = web.RouteTableDef()


@router.get("/")
@aiohttp_jinja2.template("index.html")
async def index(request: web.Request) -> Dict[str, Any]:
    """Get index page."""
    results = retrieve_file2dataset()
    return {"posts": results}


async def init_app() -> web.Application:
    """Init app."""
    app = web.Application()
    app.add_routes(router)
    aiohttp_jinja2.setup(
        app, loader=jinja2.FileSystemLoader(str(Path(__file__).parent / "templates"))
    )
    app['static_root_url'] = 'static'

    return app


def main():
    """Run the app."""
    # sslcontext.load_cert_chain(ssl_certfile, ssl_keyfile)
    # sslcontext = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    # sslcontext.check_hostname = False
    web.run_app(init_app(), host=os.environ.get('HOST', '0.0.0.0'),
                port=os.environ.get('PORT', '5050'),
                shutdown_timeout=0, ssl_context=None)


if __name__ == '__main__':
    if sys.version_info < (3, 6):
        LOG.error("this app requires python3.6")
        sys.exit(1)
    main()
