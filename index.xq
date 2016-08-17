xquery version "3.0";

declare namespace output="http://www.w3.org/2010/xslt-xquery-serialization";

declare option output:method "html5";
declare option output:media-type "text/html";

(: Determine if the aws_config module is available :)
declare function local:aws-config-installed() {
    try {
        util:import-module(xs:anyURI("http://history.state.gov/ns/xquery/aws_config"), "aws_config", xs:anyURI("/db/apps/s3/modules/aws_config.xqm")),
        true()
    } catch * {
        false()
    }
};

<div>
    <h1>S3</h1>
    <p>S3 Config Module is {if (local:aws-config-installed()) then () else 'not'} installed. From the README:</p>
    <blockquote><pre>{util:binary-to-string(util:binary-doc('/db/apps/s3/README.md'))}</pre></blockquote>
</div>