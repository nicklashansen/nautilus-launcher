import os
import sys
import re
import itertools
import tempfile
import termcolor
import string
import random
import codecs
import subprocess
from pathlib import Path

import yaml


def _get_current_context():
	cmd = ["kubectl", "config", "current-context"]
	output = subprocess.check_output(cmd)
	return output.decode().strip()


def _safe_str(name, length=8):
	name = re.sub(r'(\W)\1+', r'\1', re.sub(r'[^a-zA-Z0-9]', '-', name))
	if len(name) > length:
		name = name[:length]
	else:
		name += 'a' * (length - len(name))
	return name.lower()


def _encode_name(name):
	uid = ''.join(random.choice(string.ascii_lowercase) for _ in range(4))
	return codecs.encode((f'{_safe_str(name)}-{uid}').replace('-', ''), 'rot13')


def _read_text(fp):
	with open(fp, 'r') as f:
		text = f.read()
	return text


def _submit(args, name):
	context = _get_current_context()
	template = _read_text('nautilus/job.yaml')
	wandb_key = _read_text('nautilus/wandb.key')
	cfg = yaml.safe_load(Path(f'{os.getcwd()}/nautilus/config.yaml').read_text())
	cfg.update(
		name='nh-'+_encode_name(name),
		namespace=context,
		haosu='In' if 'haosu' in context else 'NotIn',
		wandb_key=wandb_key,
	)
	while '{{' in template:
		for key, value in cfg.items():
			regexp = r'\{\{\s*' + str(key) + r'\s*\}\}'
			template = re.sub(regexp, str(value), template)
	tmp = tempfile.NamedTemporaryFile(suffix='.yaml')
	with open(tmp.name, 'w') as f:
		f.write(template)
	print(termcolor.colored(f'{cfg["name"]}', 'yellow'), args if len(args) > 0 else 'None')
	os.system(f'kubectl create -f {tmp.name}')
	tmp.close()


def _submit_batch(kwargs: dict):
	if 'seed' in kwargs and isinstance(kwargs['seed'], list):
		seeds = ','.join(kwargs['seed'])
		del kwargs['seed']
	else:
		seeds = None
	arg_list = list(itertools.product(*kwargs.values()))
	if len(arg_list) > 32:
		print(termcolor.colored(f'Error: {len(arg_list)} jobs exceeds limit of 32', 'red'))
		return
	print(termcolor.colored(f'Submitting {len(arg_list)} job{"s" if len(arg_list) > 1 else ""}', 'green'))
	if seeds is not None:
		kwargs['seed'] = seeds
	for args in arg_list:
		if seeds is not None:
			args = (*args, seeds)
		args = ' '.join([f'{k}={v}' for k, v in zip(kwargs.keys(), args)])
		_submit(args, name=kwargs['exp_name'][0] if 'exp_name' in kwargs else 'default')


def launch():
	kwargs = dict(arg.split('=') for arg in sys.argv[1:])
	kwargs = {k: v.split(',') for k, v in kwargs.items()}
	_submit_batch(kwargs)
