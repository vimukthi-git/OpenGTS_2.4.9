// ----------------------------------------------------------------------------
// Copyright 2007-2013, GeoTelematic Solutions, Inc.
// All rights reserved
// ----------------------------------------------------------------------------
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// ----------------------------------------------------------------------------
// Change History:
//  2009/06/01  Martin D. Flynn
//     -Extracted from SendMail
//  2011/12/06  Martin D. Flynn
//     -Added "UTF-8" character set to email body text.
//  2012/08/01  Martin D. Flynn
//     -Added check for "smtp.ignoredEmail.file" (see IsIgnoredEmailAddress)
// ----------------------------------------------------------------------------
package org.opengts.util;

import java.lang.reflect.*;
import java.io.*;
import java.util.*;
import java.net.*;
import java.awt.*;
import java.awt.event.*;
import javax.activation.*; // since Java 6

import javax.mail.*;
import javax.mail.internet.*;

public class SendMailArgs
{

    // ------------------------------------------------------------------------

    public static final boolean USE_AUTHENTICATOR   = true;

    public static final String SSL_FACTORY          = "javax.net.ssl.SSLSocketFactory";

    // ------------------------------------------------------------------------

    private static Object      IgnEmailLock         = new Object();
    private static boolean     IgnEmailInit         = false;
    private static File        IgnEmailFile         = null;
    private static long        IgnEmailModTime      = 0L;
    private static Set<String> IgnEmailSet          = null;
    private static boolean     IgnEmailDebug        = false;

    private static boolean IsIgnoredEmailAddress(String e)
    {

        /* blank email address? */
        if (StringTools.isBlank(e)) {
            return true;
        }

        /* validate email address? */
        // TODO:

        /* init/reload file */
        synchronized (IgnEmailLock) {

            // initialize
            if (!IgnEmailInit) {
                File ignFile = RTConfig.getFile(RTKey.SMTP_IGNORED_EMAIL_FILE, null);
                if ((ignFile != null) && !ignFile.isAbsolute()) {
                    File dir = RTConfig.getLoadedConfigDir();
                    ignFile = (dir != null)? new File(dir,ignFile.toString()) : null;
                }
                IgnEmailFile    = ignFile; // may be null
                IgnEmailModTime = 0L;
                IgnEmailSet     = null;
                IgnEmailInit    = true;
                Print.logInfo("Ignored Email file: " + IgnEmailFile);
            }

            // reload
            if ((IgnEmailFile == null) || !IgnEmailFile.isFile()) {
                if (IgnEmailSet != null) {
                    Print.logWarn("Ignored Email file no longer exists: " + IgnEmailFile);
                    IgnEmailModTime = 0L;
                    IgnEmailSet     = null;
                }
            } else {
                long lastMod = IgnEmailFile.lastModified();
                if (lastMod == 0L) { 
                    Print.logWarn("No Ignored Email file last modified time: " + IgnEmailFile);
                    IgnEmailModTime = 0L;
                    IgnEmailSet     = null;
                } else
                if (lastMod > IgnEmailModTime) {
                    Print.logInfo("Reloading Ignored Email file: " + IgnEmailFile);
                    Set<String> ignSet = new HashSet<String>();
                    FileInputStream fis = null;
                    try {
                        fis = new FileInputStream(IgnEmailFile);
                        for (int r = 0;; r++) {
                            // read line
                            String line = null;
                            try {
                                line = FileTools.readLine(fis);
                                if (line == null) { break; } // end of file
                                line = line.trim();
                            } catch (EOFException eof) {
                                break; // end of file
                            }
                            // simple validation
                            if (line.startsWith("#")) {
                                continue; // comment
                            } else
                            if (line.indexOf("@") <= 0) {
                                continue; // not an email address
                            }
                            // add to set
                            if (IgnEmailDebug) { Print.logInfo("Adding Ignored Email Addr: " + line); }
                            ignSet.add(line);
                        }
                    } catch (IOException ioe) {
                        Print.logException("IO Error", ioe);
                    } finally {
                        if (fis != null) { try { fis.close(); } catch (Throwable th) {} }
                    }
                    // save
                    IgnEmailSet     = !ListTools.isEmpty(ignSet)? ignSet : null;
                    IgnEmailModTime = lastMod;
                }
            }

        } // synchronized (IgnEmailLock)

        /* empty ignore list */
        if (ListTools.isEmpty(IgnEmailSet)) {
            return false;
        }

        /* ignore? */
        if (IgnEmailSet.contains(e)) {
            return true;
        } else {
            return false;
        }

    }

    // ------------------------------------------------------------------------

    /** 
    *** Filters and returns the base email address from the specified String.<br>
    *** For example, if the String "Jones&lt;jones@example.com&gt;" is passed to this
    *** method, then the value "jones@example.com" will be returned.
    *** @param addr The email address to filter.
    *** @return  The filtered email address, or null if the specified email address is invalid.
    **/
    public static String parseEMailAddress(String addr)
    {
        if (!StringTools.isBlank(addr)) {
            try {
                InternetAddress ia = new InternetAddress(addr, true);
                return ia.getAddress();
            } catch (Throwable ae) { // AddressException
                Print.logWarn("Invalid EMail address: " + addr);
                return null;
            }
        } else {
            return null;
        }
    }

    // ------------------------------------------------------------------------

    /**
    *** Internal method to send email
    *** @param args  The email arguments
    *** @return True if the email was sent, false otherwise
    **/
    public static boolean send(SendMail.Args args)
    {
        String                       from = args.getFrom();
        String                       to[] = args.getTo();
        String                       cc[] = args.getCc();
        String                      bcc[] = args.getBcc();
        String                    subject = args.getSubject();
        String                    msgBody = args.getBody();
        Properties                headers = args.getHeaders();
        SendMail.Attachment        attach = args.getAttachment();
        SendMail.SmtpProperties smtpProps = args.getSmtpProperties(); // never null

        /* SMTP properties */
        // http://www.j2ee.me/products/javamail/javadocs/com/sun/mail/smtp/package-summary.html
        // mail.smtp.host (String)
        // mail.smtp.port (int)
        // mail.smtp.user (String)
        // mail.smtp.auth (boolean)
        // mail.smtp.connectiontimeout (int)  [miliseconds]
        // mail.smtp.timeout (int)  [miliseconds]
        // mail.smtp.socketFactory.class (String)
        // mail.smtp.socketFactory.port (int)
        // mail.smtp.socketFactory.fallback (boolean)
        // mail.smtp.starttls.enable (boolean)
        // mail.smtp.sendpartial (boolean)
        Properties props = new Properties();

        // Debug
        if (smtpProps.getDebug()) {
            props.put("mail.debug", "true");
            Print.logDebug("SendMail debug mode");
        }

        // SMTP Credentials
        final String smtpHost  = smtpProps.getHost();
        final int    smtpPort  = smtpProps.getPort();
        final String smtpUser  = smtpProps.getUser();
        final String smtpEmail = smtpProps.getUserEmail();
        final String smtpPass  = smtpProps.getPassword();
        final String enableSSL = smtpProps.getEnableSSL();
        final String enableTLS = smtpProps.getEnableTLS();

        // SMTP host:port
        if (StringTools.isBlank(smtpHost) || smtpHost.endsWith("example.com")) {
            Print.logError("Null/Invalid SMTP host, not sending email");
            return false;
        } else
        if (smtpPort <= 0) {
            Print.logError("Invalid SMTP port, not sending email");
            return false;
        }
        props.put("mail.smtp.host"                          , smtpHost);
        props.put("mail.smtp.port"                          , String.valueOf(smtpPort));
        props.put("mail.smtp.connectiontimeout"             , "60000");
        props.put("mail.smtp.timeout"                       , "60000");
      //props.put("mail.smtp.auth"                          , "true");
      //props.put("mail.smtp.auth.mechanisms"               , "LOGIN PLAIN DIGEST-MD5 NTLM");
      
        // The following can be used as a replacement for the value returned by
        // "InetAddress.getLocalHost().getHostName()".
      //props.put("mail.smtp.localhost"                     , "mydomain.example.com");

        // SSL
        if (enableSSL.equals("only") || enableSSL.equals("true")) {
            props.put("mail.smtp.socketFactory.port"        , String.valueOf(smtpPort));
            props.put("mail.smtp.socketFactory.class"       , SSL_FACTORY);
            props.put("mail.smtp.socketFactory.fallback"    , "false");
          //props.put("mail.smtp.socketFactory.fallback"    , "true");
            if (enableSSL.equals("only")) {
                props.put("mail.smtp.ssl.enable"            , "true");
                props.put("mail.smtp.ssl.socketFactory.port", String.valueOf(smtpPort));
            }
        }
        
        // TLS
        if (enableTLS.equals("only") || enableTLS.equals("true")) {
            props.put("mail.smtp.starttls.required"         , "true");
            props.put("mail.smtp.starttls.enable"           , "true");
        }

        /* SMTP Authenticator */
        javax.mail.Authenticator auth = null;
        if (USE_AUTHENTICATOR && !StringTools.isBlank(smtpUser)) {
            auth = new javax.mail.Authenticator() {
                public javax.mail.PasswordAuthentication getPasswordAuthentication() {
                    return new javax.mail.PasswordAuthentication(smtpUser, smtpPass);
                }
            };
            props.put("mail.smtp.user", smtpUser);
            props.put("mail.smtp.auth", "true"); // SSL
        }

        /* SMTP Session */
        //props.list(System.out);
        Session session = Session.getInstance(props, auth);

        try {
            MimeMessage msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(from));

            InternetAddress toAddr[]  = _convertRecipients(to);
            InternetAddress ccAddr[]  = _convertRecipients(cc);
            InternetAddress bccAddr[] = _convertRecipients(bcc);
            if ((toAddr != null) && (toAddr.length > 0)) {
                for (Iterator i = headers.keySet().iterator(); i.hasNext();) {
                    String k = (String)i.next();
                    String v = headers.getProperty(k);
                    if (v != null) {
                        msg.setHeader(k, v);
                    }
                }
                msg.setRecipients(Message.RecipientType.TO , toAddr);
                msg.setRecipients(Message.RecipientType.CC , ccAddr);
                msg.setRecipients(Message.RecipientType.BCC, bccAddr);
                msg.setSubject(subject, StringTools.CharEncoding_UTF_8);
                msg.setSentDate(new Date());
                if ((attach != null) && (attach.getSize() > 0)) {
                    Multipart multipart = new MimeMultipart();
                    if ((msgBody != null) && !msgBody.equals("")) {
                        MimeBodyPart textBodyPart = new MimeBodyPart();
                        textBodyPart.setText(msgBody, StringTools.CharEncoding_UTF_8);
                        multipart.addBodyPart(textBodyPart);
                    }
                    // add attachment
                    BodyPart attachBodyPart = new MimeBodyPart();
                    DataSource source = new ByteArrayDataSource(attach.getName(), attach.getType(), attach.getBytes());
                    attachBodyPart.setDataHandler(new DataHandler(source));
                    attachBodyPart.setFileName(source.getName());
                    multipart.addBodyPart(attachBodyPart);
                    // set content 
                    msg.setContent(multipart);
                } else {
                    msg.setText(msgBody, StringTools.CharEncoding_UTF_8);
                    //msg.setText(msgBody); // setContent(msgBody, CONTENT_TYPE_PLAIN);
                }

                /* send email */
                msg.saveChanges(); // implicit with send()
                if (!USE_AUTHENTICATOR && !StringTools.isBlank(smtpUser)) {
                    Transport transport = session.getTransport("smtp");
                    transport.connect(smtpHost, smtpUser, (smtpPass!=null?smtpPass:""));
                    transport.sendMessage(msg, msg.getAllRecipients());
                    transport.close();
                } else {
                    Transport.send(msg);
                }
                Print.logDebug("Email sent ...");

                return true;
            } else {
                return false;
            }

        } catch (MessagingException me) {
            
            Print.logStackTrace("Unable to send email [host="+smtpHost+"; port="+smtpPort+"]", me);
            for (Exception ex = me; ex != null;) {
                if (ex instanceof SendFailedException) {
                    SendFailedException sfex = (SendFailedException)ex;
                    _printAddresses("Invalid:"     , sfex.getInvalidAddresses());
                    _printAddresses("Valid Unsent:", sfex.getValidUnsentAddresses());
                    _printAddresses("Valid Sent:"  , sfex.getValidSentAddresses());
                }
                ex = (ex instanceof MessagingException)? ((MessagingException)ex).getNextException() : null;
            }

            return false;

        }

    }

    // ------------------------------------------------------------------------

    /**
    *** Converts the list of String email addresses to instances of 'InternetAddress'
    *** @param to  The array of email addresses
    *** @return An array of InternetAddress instances
    *** @throws AddressException if any of the specified email addresses are invalid
    **/
    private static InternetAddress[] _convertRecipients(String to[])
        throws AddressException
    {
        java.util.List<InternetAddress> inetAddr = new Vector<InternetAddress>();
        for (int i = 0; i < to.length; i++) {

            /* trim email address */
            String t = (to[i] != null)? to[i].trim() : "";
            if (t.equals("")) { 
                // Print.logWarn("Ignoring Blank Email Address");
                continue;
            }

            /* check for email address to skip */
            if (SendMailArgs.IsIgnoredEmailAddress(t)) {
                Print.logWarn("Ignoring Email Address: " + t);
                continue; 
            }

            /* convert to InternetAddress instance */
            try {
                inetAddr.add(new InternetAddress(t)); 
            } catch (AddressException ae) {
                Print.logStackTrace("Address: " + t + " (skipped)", ae);
            }

        }
        return inetAddr.toArray(new InternetAddress[inetAddr.size()]);
    }

    // ------------------------------------------------------------------------

    /**
    *** Prints the list of email addresses (debug purposes only)
    **/
    private static void _printAddresses(String msg, Address addr[])
    {
        if (addr != null) {
            Print.logInfo(msg);
            for (int i = 0; i < addr.length; i++) {
                Print.logInfo("    " + addr[i]);
            }
        }
    }

    // ------------------------------------------------------------------------

    /**
    *** ByteArrayDataSource class
    **/
    private static class ByteArrayDataSource
        implements DataSource
    {
        private String name   = null;
        private String type   = null;
        private Object source = null;
        private ByteArrayDataSource(String name, String type, Object src) {
            this.name   = name;
            this.type   = type;
            this.source = src;
        }
        public ByteArrayDataSource(String name, byte src[]) {
            this(name, null, src);
        }
        public ByteArrayDataSource(String name, String type, byte src[]) {
            this(name, type, (Object)src);
        }
        public ByteArrayDataSource(String name, String src) {
            this(name, null, src);
        }
        public ByteArrayDataSource(String name, String type, String src) {
            this(name, type, (Object)src);
        }
        public String getName() {
            return (this.name != null)? this.name : "";
        }
        public String getContentType() {
            if (this.type != null) {
                return this.type;
            } else 
            if (this.getName().toLowerCase().endsWith(".csv")) {
                return HTMLTools.MIME_CSV();
            } else 
            if (this.getName().toLowerCase().endsWith(".gif")) {
                return HTMLTools.MIME_GIF();
            } else 
            if (this.getName().toLowerCase().endsWith(".png")) {
                return HTMLTools.MIME_PNG();
            } else
            if (this.source instanceof byte[]) {
                return SendMail.DefaultContentType((byte[])this.source);
            } else
            if (this.source instanceof ByteArrayOutputStream) {
                return SendMail.DefaultContentType(((ByteArrayOutputStream)this.source).toByteArray());
            } else {
                return HTMLTools.MIME_PLAIN();
            }
        }
        public InputStream getInputStream() {
            return new ByteArrayInputStream(this.toByteArray());
        }
        public OutputStream getOutputStream() {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte b[] = this.toByteArray();
            if ((b != null) && (b.length > 0)) {
                out.write(b, 0, b.length);
            }
            this.source = out;
            return (ByteArrayOutputStream)this.source;
        }
        private byte[] toByteArray() {
            if (this.source == null) {
                return new byte[0];
            } else
            if (this.source instanceof byte[]) {
                return (byte[])this.source;
            } else
            if (this.source instanceof ByteArrayOutputStream) {
                return ((ByteArrayOutputStream)this.source).toByteArray();
            } else {
                return StringTools.getBytes(this.source.toString());
            }
        }
    }                               

    // ------------------------------------------------------------------------

}
