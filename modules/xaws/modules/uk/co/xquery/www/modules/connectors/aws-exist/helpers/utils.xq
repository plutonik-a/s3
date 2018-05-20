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

declare namespace functx = "http://www.functx.com";

declare function functx:day-of-week
  ( $date as xs:anyAtomicType? )  as xs:integer? {

  if (empty($date))
  then ()
  else xs:integer((xs:date($date) - xs:date('1901-01-06'))
          div xs:dayTimeDuration('P1D')) mod 7
 } ;
 
declare function functx:day-of-week-name-en
  ( $date as xs:anyAtomicType? )  as xs:string? {

   ('Sunday', 'Monday', 'Tuesday', 'Wednesday',
    'Thursday', 'Friday', 'Saturday')
      [functx:day-of-week($date) + 1]
 } ;
 
(:~
 : Generate a date formated according to rfc822. Example: Fri, 15 Oct 10
 :
 : cf: http://www.faqs.org/rfcs/rfc822.html
 :
 : @return rfc822 formated date as xs:string
:)
declare function aws-utils:http-date() as xs:string {

    (: TODO replace day of week with [FNn,*-3] when https://github.com/eXist-db/exist/issues/1878 is resolved :)
    let $day-of-week := substring(functx:day-of-week-name-en(current-date()), 1, 3)
    let $rest := 
        format-dateTime(adjust-dateTime-to-timezone(current-dateTime(), xs:dayTimeDuration("PT0H")), "[D] [MNn] [Y0001] [H01]:[m01]:[s01] [ZN]", "en", (), "US")
    return
        concat($day-of-week, ", ", $rest)
};

(:~
 : Generate a date formatted "YYYYMMDD'T'HHMMSS'Z'" for x-amz-date required by AWS Signature Version 4. Example: 20150830T123600Z
 :
 : @see https://docs.aws.amazon.com/general/latest/gr/sigv4-date-handling.html
 : @return x-amz-date formatted dateTime as xs:string
:)
declare function aws-utils:x-amz-date() as xs:string {

    format-dateTime(adjust-dateTime-to-timezone(current-dateTime(), xs:dayTimeDuration("PT0H")), "[Y0001][M01][D01]T[H01][m01][s01][ZZ]")
};

(:~
 : Generate a date formatted "YYYYMMDD" for date value required by AWS Signature Version 4. Example: 20150830
 :
 : @see https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
 : @return YYYYMMDD formatted date as xs:string
:)
declare function aws-utils:yyyymmdd-date() as xs:string {

    format-date(adjust-date-to-timezone(current-date(), xs:dayTimeDuration("PT0H")), "[Y0001][M01][D01]")
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
