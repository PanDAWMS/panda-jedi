from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

# Set up the task buffer interface
tb_if = JediTaskBufferInterface()
tb_if.setupInterface()

# Get the site mapper
site_mapper = tb_if.getSiteMapper()

# Define a random site of panda sites
panda_sites = ['ANALY_DESY-HH', 'ANALY_DESY-HH_TEST', 'WT2_Install', 'BNL_Test_2_CE_1', 'ANALY_IN2P3-CC-T2_RD',
               'UKI-SOUTHGRID-OX-HEP_SL6', 'RU-Protvino-IHEP', 'ANALY_NICS_Kraken', 'ANALY_TRIUMF_HIMEM',
               'TESTGLEXEC', 'ANALY_GRIF-LPNHE', 'UKI-SCOTGRID-GLASGOW_MCORE', 'BNL_PROD', 'RAL-LCG2_SL6']

# Test the getSiteInputStorageEndpointMap function
print tb_if.getSiteInputStorageEndpointMap(panda_sites, site_mapper)

for panda_site in panda_sites:
    print panda_site
    tmp_site_spec = site_mapper.getSite('ANALY_DESY-HH')
    print '------------------- ddm -------------------'
    print tmp_site_spec.ddm
    print '------------------- setokens values -------------------'
    print tmp_site_spec.setokens.values()
    print '------------------- setokens -------------------'
    print tmp_site_spec.setokens