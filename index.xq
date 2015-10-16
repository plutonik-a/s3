xquery version "3.0";

import module namespace aws_config = "http://history.state.gov/ns/xquery/aws_config" at 'modules/aws_config.xqm';
import module namespace bucket = 'http://www.xquery.co.uk/modules/connectors/aws/s3/bucket' at 'modules/xaws/modules/uk/co/xquery/www/modules/connectors/aws-exist/s3/bucket.xq';
import module namespace frus = "http://history.state.gov/ns/site/hsg/frus-html" at "/db/apps/hsg-shell/modules/frus-html.xqm";

declare namespace output="http://www.w3.org/2010/xslt-xquery-serialization";
declare namespace s3="http://s3.amazonaws.com/doc/2006-03-01/";
declare namespace httpclient="http://exist-db.org/xquery/httpclient";
declare namespace functx = "http://www.functx.com"; 

declare option output:method "html5";
declare option output:media-type "text/html";

declare function functx:substring-after-last-match 
  ( $arg as xs:string? ,
    $regex as xs:string )  as xs:string {
       
   replace($arg,concat('^.*',$regex),'')
 } ;

declare function local:contents-to-resources($contents) {
    for $item in $contents
    let $key := data($item/s3:Key)
    let $filename := functx:substring-after-last-match($key, '/')
    let $size := data($item/s3:Size)
    let $last-modified := data($item/s3:LastModified)
    return
        <resource>
            <filename>{$filename}</filename>
            <s3-key>{$key}</s3-key>
            <size>{$size}</size>
            <last-modified>{$last-modified}</last-modified>
        </resource>
};

declare function local:mkcol-recursive($collection, $components) {
    if (exists($components)) then
        let $newColl := concat($collection, "/", $components[1])
        return (
            xmldb:create-collection($collection, $components[1]),
            local:mkcol-recursive($newColl, subsequence($components, 2))
        )
    else
        ()
};

(: Helper function to recursively create a collection hierarchy. :)
declare function local:mkcol($collection, $path) {
    local:mkcol-recursive($collection, tokenize($path, "/"))
};

(: provide this function a directory like 'frus/frus1964-68v12/ebook/' and it will update 
the existing cache of that directory's contents :)
declare function local:update-leaf-directory($directory as xs:string) {
    let $bucket := 'static.history.state.gov'
    let $delimiter := '/'
    let $marker := ()
    let $max-keys := ()
    let $prefix := $directory
    let $list := bucket:list($aws_config:AWS-ACCESS-KEY, $aws_config:AWS-SECRET-KEY, $bucket, $delimiter, (), (), $prefix)
    let $contents := $list/httpclient:body/s3:ListBucketResult/s3:Contents[s3:Key ne $prefix]
    let $resources := 
        <resources prefix="{$prefix}">{
            local:contents-to-resources($contents)
        }</resources>
    let $cache-collection-base := '/db/apps/s3/cache/'
    let $target-collection := string-join(($cache-collection-base, $bucket, $prefix), '/')
    return 
        try {
            let $ensure-collections-exist := 
        if (xmldb:collection-available($target-collection)) then 
            () 
        else
            local:mkcol(
                string-join(tokenize($cache-collection-base, '/')[position() = 1 to last() - 1], '/'), 
                concat(string-join(tokenize($cache-collection-base, '/')[last()], '/'), '/', $bucket, '/', $prefix)
                )
            return
                (
                <p class="bg-success">Stored {xmldb:store($target-collection, 'resources.xml', $resources)}</p>
                ,
                <pre>{serialize($resources)}</pre>
                )
        } catch * {
                let $error := concat('Error while fetching S3 Resources for ', $directory, ': ', 
                        $err:code, $err:value, " module: ",
                        $err:module, "(", $err:line-number, ",", $err:column-number, ") ", $err:description
                        )
                return
                    <p class="bg-danger">{$error}</p>
            }
};

declare function local:update-leaf-directories($directories as xs:string+) {
    $directories ! local:update-leaf-directory(.)
};

declare function local:wrap-html($title, $body) {
    <html>
        <head>
            <title>{$title}</title>
            <meta name="viewport" content="width=device-width, initial-scale=1"/>
            <!-- Latest compiled and minified CSS -->
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap.min.css"/>
            
            <!-- Optional theme -->
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap-theme.min.css"/>
            <link rel="stylesheet" href="quarterly-release-print.css"/>
            <!-- Latest compiled and minified JavaScript -->
            <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/js/bootstrap.min.js"/>
        </head>
        <body>
            <div class="container">
                <h1>{$title}</h1>
                {$body}
            </div>
        </body>
    </html>
};

declare function local:form($volumes as xs:string*) {
    <div class="form-group">
        <form action="{request:get-uri()}">
            <label for="volumes" class="control-label">Volume IDs, one per line</label>
            <div>
                <textarea name="volumes" id="volumes" class="form-control" rows="6">{$volumes}</textarea>
            </div>
            <button type="submit" class="btn btn-default">Submit</button>
            <a class="btn btn-default" href="{request:get-uri()}" role="button">Clear</a>
        </form>
    </div>
};

declare function local:validate($vol-ids as xs:string*) {
    for $vol-id in $vol-ids
    return
        if (frus:exists-volume($vol-id)) then () 
        else $vol-id
};

let $title := 'S3 Cache Helper'
let $new-volumes := request:get-parameter('volumes', ())
let $body := 
    if ($new-volumes) then
        (
        local:form($new-volumes),
        let $vol-ids := 
            for $vol-id in tokenize($new-volumes, '\s+')[. ne '']
            order by $vol-id
            return $vol-id
        let $invalid-ids := local:validate($vol-ids)
        return
            if (empty($invalid-ids)) then
                for $vol-id in $vol-ids
                let $directories := ('ebook', 'pdf') ! concat('frus/', $vol-id, '/', ., '/')
                return
                    local:update-leaf-directories($directories)
            else
                <div class="bg-danger">
                    <p>The following volume ID(s) are invalid. Please correct the ID and resubmit.</p>
                    <ul>{
                        for $vol-id in $invalid-ids
                        return
                            <li>{$vol-id}</li>
                    }</ul>
                </div>
        )
    else
        (
        local:form(()),
        <p>This utility will retrieve the latest information about FRUS PDFs and Ebooks from S3, to ensure the website has up-to-date links and file sizes for these resources.</p>,
        <p>Please enter volume IDs, one per line. (Click <a href="?volumes=frus1969-76v13">here</a> to try.)</p>
        )
return
    local:wrap-html($title, $body)