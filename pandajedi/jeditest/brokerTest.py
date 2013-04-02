from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

siteMapper = tbIF.getSiteMapper()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()
atlasIF = ddmIF.getInterface('atlas')

from pandajedi.jedibrokerage.AtlasProdJobBroker import AtlasProdJobBroker
br = AtlasProdJobBroker(atlasIF,tbIF,siteMapper)

st,taskSpec = tbIF.getTaskWithID_JEDI(1)
print br.doBrokerage(taskSpec,'US',
  #{'mc12_8TeV.167751.Sherpa_CT10_ZeeMassiveCBPt0_CVetoBVeto.recon.AOD.e1585_a159_a171_tid01225039_00':['AOD.01225039._002187.pool.root.1','AOD.01225039._002627.pool.root.1']})
  {'data11_7TeV.00190256.physics_JetTauEtmiss.merge.NTUP_SMWZ.f408_m1007_p1035_tid01048572_00':['NTUP_SMWZ.01048572._000522.root.1']})