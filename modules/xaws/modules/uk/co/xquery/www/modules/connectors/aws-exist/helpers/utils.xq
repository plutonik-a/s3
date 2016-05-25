(:
 : Copyright 2010 XQuery.co.uk
 :
 : Licensed under the Apache License, Version 2.0 (the "License");
 : you may not use this file except in compliance with the License.
 : You may obtain a copy of the License at
 :
 : http://www.apache.org/licenses/LICENSE-2.0
 :
 : Unless required by applicable law or agreed to in writing, software
 : distributed under the License is distributed on an "AS IS" BASIS,
 : WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 : See the License for the specific language governing permissions and
 : limitations under the License.
:)

(:~
 : <p>
 : </p>
 :
 : @author Klaus Wichmann klaus [at] xquery [dot] co [dot] uk
 :)
module namespace aws-utils = 'http://www.xquery.co.uk/modules/connectors/aws/helpers/utils';

import module namespace xsl = 'http://www.w3.org/1999/XSL/Transform';

(:~
 : Generate a date formated according to rfc822. Example: Fri, 15 Oct 10
 :
 : cf: http://www.faqs.org/rfcs/rfc822.html
 :
 : @return rfc822 formated date as xs:string
:)
declare function aws-utils:http-date() as xs:string {

    (: note: two functions can do this in eXist:
        1. xsl:format-dateTime(), which uses the XML Schema "Picture Format"  
            http://www.w3.org/TR/xslt20/#date-picture-string
        2. datetime:format-dateTime(), which uses the Java SimpleDateFormat format:
            http://download.oracle.com/javase/6/docs/api/index.html?java/text/SimpleDateFormat.html
    :)    
    
    format-dateTime(adjust-dateTime-to-timezone(current-dateTime(), xs:dayTimeDuration('PT0H')), "[F,*-3], [D] [MNn] [Y0001] [H01]:[m01]:[s01] +0000", 'en', (), 'US')
    (:
    xsl:format-dateTime(adjust-dateTime-to-timezone(current-dateTime(), xdt:dayTimeDuration("PT8H")), "F, DD MMMM YYYY HH:mm:ss Z")
    :)
    (: alternately: :)
    (:
    datetime:format-dateTime(adjust-dateTime-to-timezone(current-dateTime(), xdt:dayTimeDuration("PT8H")), 'EEE, dd MMM yyyy kk:mm:ss Z')
    :)
    
};

(:~
 : Generate a date formated: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
 :
 : @return rfc822 formated date as xs:string
:)
(: TODO-eXist :)
(:declare function utils:timestamp() as xs:string {

    let $format as xs:string := "[Y0001]-[M01]-[D01]T[H01]:[m01]:[s01].[f001]Z" 
    return 
        format-dateTime(
            adjust-dateTime-to-timezone(current-dateTime(),xs:dayTimeDuration('-PT0H'))
            ,$format, "en", (), ()
        )
};:)

(: TODO-eXist :)
(:declare sequential function utils:sleep($sec as xs:integer) {

    declare $duration as xs:dayTimeDuration := xs:dayTimeDuration(concat('PT',string($sec),'S'));
    declare $start-time := date:current-dateTime();
    declare $run-time as xs:dayTimeDuration := xs:dayTimeDuration('PT0S');
    
    while ($run-time < $duration) {
        set $run-time := 
            let $time := date:current-dateTime()
            return
                $time - $start-time;  
    }
};:)
