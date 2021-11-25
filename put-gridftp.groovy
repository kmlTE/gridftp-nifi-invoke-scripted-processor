// NiFi
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.util.StandardValidators

// jGlobus
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl
import org.globus.gsi.X509Credential
import java.io.InputStream
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import org.ietf.jgss.GSSCredential
import org.globus.io.streams.GridFTPOutputStream
import org.apache.commons.lang3.StringUtils

class PutGridFTPProcessor extends AbstractProcessor {

    // Properties
    static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
        .name("host")
        .displayName("Host")
        .description("GridFTP host address")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .build()
    static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("port")
        .displayName("Port")
        .description("GridFTP port")
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .required(true)
        .defaultValue("2811")
        .build()
    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("username")
        .displayName("Username")
        .description("GridFTP username")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .build()
    static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
        .name("path")
        .displayName("Path")
        .description("Path to the directory")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .defaultValue("~/")
        .build()
    static final PropertyDescriptor USERCERT = new PropertyDescriptor.Builder()
        .name("usercert")
        .displayName("Usercert")
        .description("User certificate")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .sensitive(true)
        .build()
    static final PropertyDescriptor USERKEY = new PropertyDescriptor.Builder()
        .name("userkey")
        .displayName("Userkey")
        .description("User certificate key")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .sensitive(true)
        .build()
    static final PropertyDescriptor BUFFER = new PropertyDescriptor.Builder()
        .name("buffer")
        .displayName("Buffer Size")
        .description("Buffer for reading the input FlowFile")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .defaultValue("8192")
        .build()

    def REL_SUCCESS = new Relationship.Builder().name("success").description('FlowFiles that were successfully processed are routed here').build()
    def REL_FAILURE = new Relationship.Builder().name("failure").description('FlowFiles are routed here if an error occurs during processing').build()

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        def properties = [] as ArrayList
        properties.add(HOST)
        properties.add(PORT)
        properties.add(USERNAME)
        properties.add(PATH)
        properties.add(USERCERT)
        properties.add(USERKEY)
        properties.add(BUFFER)
        properties
    }

    @Override
    Set<Relationship> getRelationships() {
       [REL_SUCCESS, REL_FAILURE] as Set<Relationship>
    }

    @Override
    void onTrigger(ProcessContext context, ProcessSession session) {

        FlowFile flowFile = session.get()
        if (flowFile == null) {
            return;
        }

        String destHostname = context.getProperty(HOST).getValue()
        int destPort = context.getProperty(PORT).asInteger()
        String destPath = StringUtils.removeEnd(context.getProperty(PATH).getValue(), "/") + "/" + flowFile.getAttribute("filename")

        try {

            GlobusGSSCredentialImpl gss_cred = null

            InputStream USERCERT_STREAM = new ByteArrayInputStream(context.getProperty(USERCERT).getValue().getBytes(StandardCharsets.US_ASCII))
            InputStream USERKEY_STREAM = new ByteArrayInputStream(context.getProperty(USERKEY).getValue().getBytes(StandardCharsets.US_ASCII))
            X509Credential x509_cred = new X509Credential(
                USERCERT_STREAM, USERKEY_STREAM
            )
            gss_cred = new GlobusGSSCredentialImpl(
                x509_cred, GSSCredential.DEFAULT_LIFETIME
            )

            GridFTPOutputStream output_stream = new GridFTPOutputStream(
                gss_cred,
                destHostname,
                destPort,
                destPath,
                false
            );

            session.read(flowFile, {inputStream ->
                byte[] buf = new byte[context.getProperty(BUFFER).asInteger()]
                int length
                try {
                    while ((length = inputStream.read(buf)) > 0) {
                        output_stream.write(buf, 0, length)
                        output_stream.flush()
                    }
                } finally {
                    output_stream.close()
                }
            } as InputStreamCallback)

            session.transfer(flowFile, REL_SUCCESS)
            
        } catch (final Exception e) {
            getLogger().error('Failed to download {}; will route to failure', [e] as Object[])
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        getLogger().info('Successfully downloaded')
    }
}

processor = new PutGridFTPProcessor()