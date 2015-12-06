xquery version "3.0";

import module namespace unzip = "http://joewiz.org/ns/xquery/unzip" at "https://raw.githubusercontent.com/joewiz/unzip/master/unzip/unzip.xql";

unzip:unzip('/db/hsg-temp/static.history.state.gov-ebooks-s3-cache.zip', '/db/apps/s3/cache')
