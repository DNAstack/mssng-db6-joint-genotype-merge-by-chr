workflow mergeShards {
	Array [File] partial_vcfs
	Array [File] partial_vcf_indices
	String joint_samplename
	File gvcf_URLs
	# a .bed file containing the desired regions only
	File region

	# Known sites
	File dbsnp_vcf
	File dbsnp_index
	File ref_alt
	File ref_fasta
	File ref_fasta_index
	File ref_dict
	File ref_bwt
	File ref_sa
	File ref_amb
	File ref_ann
	File ref_pac
	File mills_vcf
	File mills_vcf_index
	File hapmap_vcf
	File hapmap_vcf_index
	File omni_vcf
	File omni_vcf_index
	File onekg_vcf
	File onekg_vcf_index
	File axiom_poly_vcf
	File axiom_poly_vcf_index

	# Sentieon License configuration
	File? sentieon_license_file
	String sentieon_license_server = ""
	Boolean use_instance_metadata = false
	String? sentieon_auth_mech
	String? sentieon_license_key

	# Execution configuration
	String threads = "4"
	String memory = "15 GB"
	String sentieon_version = "201808.06"
	String docker = "dnastack/sentieon-bcftools:${sentieon_version}"


	call mergePartialVCFs {
		input:
			partial_vcfs = partial_vcfs,
			partial_vcf_indices = partial_vcf_indices,
			joint_samplename = joint_samplename,
			gvcf_URLs = gvcf_URLs,
			region = region,
			# Known sites
			dbsnp_vcf = dbsnp_vcf,
			dbsnp_index = dbsnp_index,
			mills_vcf = mills_vcf,
			mills_vcf_index = mills_vcf_index,
			hapmap_vcf = hapmap_vcf,
			hapmap_vcf_index = hapmap_vcf_index,
			omni_vcf = omni_vcf,
			omni_vcf_index = omni_vcf_index,
			onekg_vcf = onekg_vcf,
			onekg_vcf_index = onekg_vcf_index,
			axiom_poly_vcf = axiom_poly_vcf,
			axiom_poly_vcf_index = axiom_poly_vcf_index,
			# Reference files
			ref_fasta = ref_fasta,
			ref_fasta_index = ref_fasta_index,
			ref_dict = ref_dict,
			ref_alt = ref_alt,
			ref_bwt = ref_bwt,
			ref_sa = ref_sa,
			ref_amb = ref_amb,
			ref_ann = ref_ann,
			ref_pac = ref_pac,
			# Sentieon License configuration
			sentieon_license_server = sentieon_license_server,
			sentieon_license_file = sentieon_license_file,
			use_instance_metadata = use_instance_metadata,
			sentieon_auth_mech = sentieon_auth_mech,
			sentieon_license_key = sentieon_license_key,
			# Execution configuration
			threads = threads,
			memory = memory,
			docker = docker
	}

	output {
		File GVCFtyper_main_vcf = mergePartialVCFs.GVCFtyper_main_vcf
		File GVCFtyper_main_vcf_index = mergePartialVCFs.GVCFtyper_main_vcf_index
		File GVCFtyper_split_vcf = mergePartialVCFs.GVCFtyper_split_vcf
		File GVCFtyper_split_vcf_index = mergePartialVCFs.GVCFtyper_split_vcf_index
		File split_conf = mergePartialVCFs.split_conf
	}

	meta {
    author: "Heather Ward"
    email: "heather@dnastack.com"
    description: "## MSSNG DB6 Joint Genotyping Mergbe By Chromosome Pipeline\n\nMerge shards corresponding to a single chromosome, then extract just those regions. `gvcf_URLs` is still required only for sample information. `region` should be a .bed file containing only single chromosome regions (e.g. `chr1.bed` contains `chr1	1	248956422`) (these files for GRCh38 are included in `bed_region_files/`). This will output one `GVCFtyper_main` and one `GVCFtyper_file` file per chromosome. The `GVCFtyper_main` file contains only columns 1-9 of a valid VCF; the `GVCFtyper_file` file contains all sample calls ([Sentieon documentation for more details](https://support.sentieon.com/appnotes/distributed_mode/)). This allows VQSR to be performed on the (much much smaller) `GVCFtyper_main` file, rather than on the entire final VCF.\n\n#### Running Sentieon\n\nIn order to use Sentieon, you must possess a license, distributed as either a key, a server, or a gcp project. The license may be attained by contacting Sentieon, and must be passed as an input to this workflow."
  }
}

task mergePartialVCFs {
	Array [File] partial_vcfs
	Array [File] partial_vcf_indices
	String joint_samplename
	File gvcf_URLs
	File region
	String chromosome = basename(region)

	# Known sites
	File? dbsnp_vcf
	File? dbsnp_index

	File mills_vcf
	File mills_vcf_index
	File hapmap_vcf
	File hapmap_vcf_index
	File omni_vcf
	File omni_vcf_index
	File onekg_vcf
	File onekg_vcf_index
	File axiom_poly_vcf
	File axiom_poly_vcf_index

	# Reference files
	File ref_fasta
	File ref_fasta_index
	File ref_dict
	File ref_alt
	File ref_bwt
	File ref_sa
	File ref_amb
	File ref_ann
	File ref_pac

	# Sentieon License configuration
	File? sentieon_license_file
	String sentieon_license_server
	Boolean use_instance_metadata
	String? sentieon_auth_mech
	String? sentieon_license_key

	# Execution configuration
	String threads
	String memory
	String docker


	command {
		set -exo pipefail
		mkdir -p /tmp
		export TMPDIR=/tmp

		# License server setup
		license_file=${default="" sentieon_license_file}
		if [[ -n "$license_file" ]]; then
		  # Using a license file
		  export SENTIEON_LICENSE=${default="" sentieon_license_file}
		elif [[ -n '${true="yes" false="" use_instance_metadata}' ]]; then
		  python /opt/sentieon/gen_credentials.py ~/credentials.json ${default="''" sentieon_license_key} &
		  sleep 5
		  export SENTIEON_LICENSE=${default="" sentieon_license_server}
		  export SENTIEON_AUTH_MECH=${default="" sentieon_auth_mech}
		  export SENTIEON_AUTH_DATA=~/credentials.json
		  read -r SENTIEON_JOB_TAG < ~/credentials.json.project
		  export SENTIEON_JOB_TAG
		else
		  export SENTIEON_LICENSE=${default="" sentieon_license_server}
		  export SENTIEON_AUTH_MECH=${default="" sentieon_auth_mech}
		fi

		# Optimizations
		export MALLOC_CONF=lg_dirty_mult:-1

		mv ${sep=' ' partial_vcfs} ${sep=' ' partial_vcf_indices} .

		# Generate split.conf file
		for line in $(cat ${gvcf_URLs})
		do
			sample=$(basename $line _Haplotyper.g.vcf.gz)
			echo $sample >> all_samples.txt
		done

		# we want all samples in a single file, and a 'main' file that can undergo VQSR
		split -d -l 100000 all_samples.txt group
		
		samples=$(cat group00 | sed 's/$/\t/g' | tr -d '\n' | sed 's/\t$//')
		echo -e "${joint_samplename}_GVCFtyper_file.vcf.gz\t$samples" >> split.conf

		# Merge GVCFs
		sentieon driver \
			--passthru \
			--algo GVCFtyper \
			--split_by_sample split.conf \
			--merge \
			${joint_samplename}_GVCFtyper_main.vcf.gz \
			$(ls *.vcf.gz)

		# Extract only the desired region from the main and samples files
		bcftools view \
			-R ${region} \
			-O z \
			-o ${joint_samplename}_GVCFtyper_file_${chromosome}.vcf.gz \
			${joint_samplename}_GVCFtyper_file.vcf.gz

		bcftools view \
			-R ${region} \
			-O z \
			-o ${joint_samplename}_GVCFtyper_main_${chromosome}.vcf.gz \
			${joint_samplename}_GVCFtyper_main.vcf.gz

		sentieon util vcfindex ${joint_samplename}_GVCFtyper_file_${chromosome}.vcf.gz
		sentieon util vcfindex ${joint_samplename}_GVCFtyper_main_${chromosome}.vcf.gz
	}

	output {
		File GVCFtyper_main_vcf = "${joint_samplename}_GVCFtyper_main_${chromosome}.vcf.gz"
		File GVCFtyper_main_vcf_index = "${joint_samplename}_GVCFtyper_main_${chromosome}.vcf.gz.tbi"
		File GVCFtyper_split_vcf = "${joint_samplename}_GVCFtyper_file_${chromosome}.vcf.gz"
		File GVCFtyper_split_vcf_index = "${joint_samplename}_GVCFtyper_file_${chromosome}.vcf.gz.tbi"
		File split_conf = "split.conf"
	}

	# No preemptible; will take > 24 hours
	runtime {
		docker: docker
		cpu: threads
		memory: memory
		disks: "local-disk 4000 HDD"
	}

}