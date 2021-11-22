// NiFi
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.util.StandardValidators

// jGlobus
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl
import org.globus.gsi.gssapi.auth.HostAuthorization
import org.globus.gsi.X509Credential
import org.globus.ftp.GridFTPClient
import org.globus.ftp.GridFTPSession
import org.globus.ftp.DataChannelAuthentication
import org.globus.ftp.Session
import org.globus.ftp.MlsxEntry
import java.io.InputStream
import java.io.ByteArrayInputStream
import java.util.Vector
import java.nio.charset.StandardCharsets
import org.ietf.jgss.GSSCredential
import org.apache.commons.lang3.StringUtils

class ListGridFTPProcessor extends AbstractProcessor {

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
        .description("Path to the file or directory")
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

    def REL_SUCCESS = new Relationship.Builder().name("success").description('FlowFiles that were successfully processed are routed here').build()

    GridFTPClient client;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        def properties = [] as ArrayList
        properties.add(HOST)
        properties.add(PORT)
        properties.add(USERNAME)
        properties.add(PATH)
        properties.add(USERCERT)
        properties.add(USERKEY)
        properties
    }

    @Override
    Set<Relationship> getRelationships() {
       [REL_SUCCESS] as Set<Relationship>
    }

    private void initClient(String destHostname, int destPort, GlobusGSSCredentialImpl gss_cred) {
        if (client == null) {
            client = new GridFTPClient(destHostname, destPort)
            client.setAuthorization(new HostAuthorization("host"))
            client.setControlChannelProtection(GridFTPSession.PROTECTION_PRIVATE)
            client.authenticate(gss_cred)
            client.setDataChannelAuthentication(DataChannelAuthentication.SELF)
            client.setType(Session.TYPE_IMAGE)
        }
    }

    @Override
    void onTrigger(ProcessContext context, ProcessSession session) {
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


            String destHostname = context.getProperty(HOST).getValue()
            int destPort = context.getProperty(PORT).asInteger()
            String destPath = context.getProperty(PATH).getValue()
            String normalizedDestPath = StringUtils.removeEnd(destPath, "/")

            initClient(destHostname, destPort, gss_cred)

            client.setPassive()
            client.setLocalActive()

            Vector destFiles = client.mlsd(destPath)
            for (int i = 0; i < destFiles.size(); i++) {
                MlsxEntry file = new MlsxEntry(destFiles.get(i).toString())
                final Map<String, String> attributes = new HashMap<>()
                String abspath = destFiles.size() == 1
                                    ? normalizedDestPath
                                    : normalizedDestPath + "/" + file.getFileName()
                String uri = "gsiftp://" + destHostname + ":" + destPort + "/" + abspath
                
                attributes.put(CoreAttributes.FILENAME.key(), file.getFileName())
                attributes.put("gridftp.absolute_path", abspath)
                attributes.put("gridftp.file_uri", uri)
                attributes.put("gridftp.size", file.get(MlsxEntry.SIZE))
                attributes.put("gridftp.modify", file.get(MlsxEntry.MODIFY))
                attributes.put("gridftp.create", file.get(MlsxEntry.CREATE))
                attributes.put("gridftp.type", file.get(MlsxEntry.TYPE))
                attributes.put("gridftp.unique", file.get(MlsxEntry.UNIQUE))
                attributes.put("gridftp.perm", file.get(MlsxEntry.PERM))
                attributes.put("gridftp.lang", file.get(MlsxEntry.LANG))
                attributes.put("gridftp.media_type", file.get(MlsxEntry.MEDIA_TYPE))
                attributes.put("gridftp.charset", file.get(MlsxEntry.CHARSET))
                attributes.put("gridftp.unix_mode", file.get(MlsxEntry.UNIX_MODE))
                attributes.put("gridftp.unix_owner", file.get(MlsxEntry.UNIX_OWNER))
                attributes.put("gridftp.unix_group", file.get(MlsxEntry.UNIX_GROUP))
                attributes.put("gridftp.unix_slink", file.get(MlsxEntry.UNIX_SLINK))
                attributes.put("gridftp.unix_uid", file.get(MlsxEntry.UNIX_UID))
                attributes.put("gridftp.unix_gid", file.get(MlsxEntry.UNIX_GID))
                attributes.put("gridftp.error", file.get(MlsxEntry.ERROR))
                attributes.put("gridftp.type_file", file.get(MlsxEntry.TYPE_FILE))
                attributes.put("gridftp.type_cdir", file.get(MlsxEntry.TYPE_CDIR))
                attributes.put("gridftp.type_pdir", file.get(MlsxEntry.TYPE_PDIR))
                attributes.put("gridftp.type_dir", file.get(MlsxEntry.TYPE_DIR))
                attributes.put("gridftp.type_slink", file.get(MlsxEntry.TYPE_SLINK))
                
                FlowFile flowFile = session.create()
                flowFile = session.putAllAttributes(flowFile, attributes)
                session.transfer(flowFile, REL_SUCCESS)
                session.commit()
            }
            
            
        } catch (final Exception e) {
            getLogger().error('Failed to list {}; will route to failure', [e] as Object[])
            client.close()
            session.rollback()
            context.yield()
            return;
        }
        getLogger().info('Successfully listed')
    }
}

processor = new ListGridFTPProcessor()