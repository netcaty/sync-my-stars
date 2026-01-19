#!/usr/bin/env python3
"""
GitHub starred仓库同步到Codeberg
修复同名仓库冲突，包含推送成功时的冲突检测
"""

import os
import sys
import json
import re
import requests
import subprocess
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

def sanitize_urls(text: str) -> str:
    """脱敏URL中的token"""
    if not text:
        return text
    
    # 匹配 https://token@domain 格式
    text = re.sub(r'(https?://)[^:/@]+@', r'\1***@', text)
    
    # 匹配 https://username:token@domain 格式
    text = re.sub(r'(https?://[^:/@]+):[^/@]+@', r'\1:***@', text)
    
    return text

def log(message: str, sanitize: bool = True):
    """安全的日志函数"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if sanitize:
        message = sanitize_urls(message)
    
    print(f"[{timestamp}] {message}")

def run_command(cmd: List[str], cwd: Optional[Path] = None) -> Tuple[bool, str]:
    """执行 shell 命令并返回结果"""
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()
        
        if result.returncode != 0:
            return False, stderr
        
        return True, stdout
        
    except subprocess.TimeoutExpired:
        return False, "命令超时"
    except Exception as e:
        return False, f"执行命令时出错: {str(e)}"

class SyncManager:
    def __init__(self):
        # 从环境变量读取配置
        self.github_token = os.getenv('GITHUB_TOKEN')
        self.codeberg_username = os.getenv('CODEBERG_USERNAME')
        self.codeberg_token = os.getenv('CODEBERG_TOKEN')
        self.full_sync = os.getenv('FULL_SYNC', 'false').lower() == 'true'
        
        # 验证配置
        if not self.github_token:
            log("错误: 缺少 GITHUB_TOKEN 环境变量")
            sys.exit(1)
        if not self.codeberg_username:
            log("错误: 缺少 CODEBERG_USERNAME 环境变量")
            sys.exit(1)
        if not self.codeberg_token:
            log("错误: 缺少 CODEBERG_TOKEN 环境变量")
            sys.exit(1)
        
        # API 配置
        self.github_headers = {
            'Authorization': f'token {self.github_token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        self.codeberg_headers = {
            'Authorization': f'token {self.codeberg_token}',
            'Content-Type': 'application/json'
        }
        
        # 工作目录
        self.workspace = Path('.')
        
        # 状态文件路径
        self.state_file = self.workspace / '.github' / 'sync_state.json'
        self.state_file.parent.mkdir(exist_ok=True)
        
        # 临时存储仓库的目录
        self.repos_dir = Path('/tmp/github-backup/repos')
        self.repos_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载状态
        self.state = self.load_state()
        
        # 统计信息
        self.stats = {
            'total': 0, 'new': 0, 'updated': 0,
            'skipped': 0, 'failed': 0, 'renamed': 0,
            'start_time': datetime.now()
        }
        
        log(f"加载了 {len(self.state)} 个仓库的状态")
    
    def load_state(self) -> Dict:
        """从项目根目录加载状态文件"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    
                    # 处理状态文件格式
                    if 'repositories' in data:
                        return data['repositories']
                    else:
                        # 旧格式，转换为新格式
                        new_state = {}
                        for repo_name, repo_info in data.items():
                            new_state[repo_name] = {
                                'name': repo_info.get('name', repo_name.split('/')[-1]),
                                'codeberg_name': repo_info.get('codeberg_name', repo_info.get('name', repo_name.split('/')[-1])),
                                'last_updated': repo_info.get('last_updated', ''),
                                'last_synced': repo_info.get('last_synced', ''),
                                'operation': repo_info.get('operation', 'unknown'),
                                'renamed': repo_info.get('renamed', False)
                            }
                        return new_state
                    
            except Exception as e:
                log(f"加载状态文件失败: {e}")
                return {}
        else:
            log("状态文件不存在，创建新的")
            return {}
    
    def save_state(self):
        """保存状态文件到项目根目录"""
        try:
            data_to_save = {
                'metadata': {
                    'last_updated': datetime.now().isoformat(),
                    'total_repos': len(self.state),
                    'renamed_repos': self.stats['renamed']
                },
                'repositories': self.state
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(data_to_save, f, indent=2)
            
            log(f"状态已保存，包含 {len(self.state)} 个仓库")
            
        except Exception as e:
            log(f"保存状态文件失败: {e}")
    
    def get_starred_repos(self) -> List[Dict]:
        """获取所有starred仓库"""
        repos = []
        page = 1
        
        log("获取GitHub starred仓库列表...")
        
        while True:
            try:
                url = "https://api.github.com/user/starred"
                params = {'page': page, 'per_page': 100}
                response = requests.get(
                    url,
                    headers=self.github_headers,
                    params=params
                )
                
                if response.status_code != 200:
                    log(f"获取失败: {response.status_code}")
                    break
                
                page_repos = response.json()
                if not page_repos:
                    break
                
                for repo in page_repos:
                    if repo:
                        repos.append({
                            'full_name': repo.get('full_name', ''),
                            'name': repo.get('name', ''),
                            'updated_at': repo.get('updated_at', ''),
                            'clone_url': repo.get('clone_url', ''),
                            'description': repo.get('description', '')
                        })
                
                log(f"已获取第 {page} 页，共 {len(repos)} 个仓库")
                page += 1
                
                # 检查是否还有更多页面
                link_header = response.headers.get('Link', '')
                if 'rel="next"' not in link_header:
                    break
                    
                time.sleep(0.5)
                
            except Exception as e:
                log(f"获取仓库列表出错: {e}")
                break
        
        log(f"总共获取到 {len(repos)} 个starred仓库")
        return repos
    
    def should_sync_repo(self, repo_info: Dict) -> bool:
        """判断是否需要同步仓库"""
        if self.full_sync:
            return True
        
        repo_name = repo_info['full_name']
        repo_updated = repo_info['updated_at']
        
        if repo_name in self.state:
            if self.state[repo_name].get('last_updated') == repo_updated:
                return False
        
        return True
    
    def codeberg_repo_exists(self, repo_name: str) -> bool:
        """检查Codeberg上是否已存在仓库"""
        url = f"https://codeberg.org/api/v1/repos/{self.codeberg_username}/{repo_name}"
        try:
            response = requests.get(
                url,
                headers=self.codeberg_headers,
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            log(f"检查Codeberg仓库失败: {e}")
            return False
    
    def create_codeberg_repo(self, repo_name: str, description: str = "") -> bool:
        """在Codeberg上创建仓库"""
        url = "https://codeberg.org/api/v1/user/repos"
        data = {
            'name': repo_name,
            'description': description[:255] if description else f"GitHub mirror",
            'private': False
        }
        
        try:
            response = requests.post(
                url,
                headers=self.codeberg_headers,
                json=data,
                timeout=30
            )
            
            if response.status_code == 201:
                log(f"✓ 在Codeberg创建仓库: {repo_name}")
                return True
            elif response.status_code == 409:
                log(f"✓ 仓库已存在: {repo_name}")
                return True
            else:
                log(f"✗ 创建仓库失败 {response.status_code}: {response.text[:100]}")
                return False
                
        except Exception as e:
            log(f"✗ 创建Codeberg仓库时出错: {e}")
            return False
    
    def is_name_already_used(self, codeberg_name: str, current_repo_full_name: str) -> bool:
        """检查Codeberg仓库名是否已被其他GitHub仓库使用"""
        for repo_full_name, repo_state in self.state.items():
            if repo_full_name != current_repo_full_name:
                # 比较时忽略大小写，因为Codeberg可能不区分大小写
                if repo_state.get('codeberg_name', '').lower() == codeberg_name.lower():
                    return True
        return False
    
    def sync_repository(self, repo_info: Dict) -> bool:
        """同步单个仓库 - 增强冲突检测"""
        repo_full_name = repo_info['full_name']
        original_name = repo_info['name']
        
        # 本地目录使用原始仓库名
        repo_path = self.repos_dir / original_name
        
        try:
            # 克隆或更新
            if repo_path.exists():
                log(f"  更新仓库...")
                success, output = run_command(['git', 'fetch', '--all'], cwd=repo_path)
                if not success:
                    log(f"  更新失败: {output}")
                    return False
                operation = "updated"
            else:
                log(f"  克隆仓库...")
                clone_url = f"https://{self.github_token}@github.com/{repo_full_name}.git"
                success, output = run_command(['git', 'clone', '--mirror', clone_url, str(repo_path)])
                if not success:
                    log(f"  克隆失败: {output}")
                    return False
                operation = "cloned"
            
            # 确定Codeberg仓库名
            if repo_full_name in self.state:
                # 使用历史记录的名称
                state_data = self.state[repo_full_name]
                codeberg_name = state_data.get('codeberg_name', original_name)
                log(f"  使用历史Codeberg名称: {codeberg_name}")
                is_renamed = state_data.get('renamed', False)
            else:
                # 首次同步，使用原始名称
                codeberg_name = original_name
                log(f"  首次尝试名称: {codeberg_name}")
                is_renamed = False
            
            # 设置远程
            codeberg_url = f"https://{self.codeberg_username}:{self.codeberg_token}@codeberg.org/{self.codeberg_username}/{codeberg_name}.git"
            
            # 检查并设置远程
            success, output = run_command(['git', 'remote'], cwd=repo_path)
            if success:
                if 'codeberg' in output:
                    # 更新远程URL
                    run_command(['git', 'remote', 'set-url', 'codeberg', codeberg_url], cwd=repo_path)
                else:
                    # 添加远程
                    run_command(['git', 'remote', 'add', 'codeberg', codeberg_url], cwd=repo_path)
            
            # 检查仓库是否存在
            if self.codeberg_repo_exists(codeberg_name):
                log(f"  仓库已存在，尝试推送...")
                
                # 尝试非强制推送
                success, output = run_command(['git', 'push', 'codeberg', '--all'], cwd=repo_path)
                
                if success:
                    # 推送成功，需要检查这个名称是否已被其他仓库使用
                    log(f"  推送成功，检查名称冲突...")
                    
                    if self.is_name_already_used(codeberg_name, repo_full_name):
                        # 名称已被其他仓库使用，需要重命名
                        log(f"  名称 {codeberg_name} 已被其他仓库使用，需要重命名")
                        return self._handle_repo_conflict(repo_info, repo_path, original_name, is_renamed)
                    else:
                        # 名称未被使用，正常处理
                        log(f"  名称 {codeberg_name} 可用")
                else:
                    # 推送失败，检查原因
                    if 'non-fast-forward' in output or 'rejected' in output:
                        # 不是同一个仓库，需要重命名
                        log(f"  检测到仓库冲突，需要重命名")
                        return self._handle_repo_conflict(repo_info, repo_path, original_name, is_renamed)
                    else:
                        # 其他错误，尝试强制推送
                        log(f"  其他错误，尝试强制推送: {output[:100]}")
                        success, output = run_command(['git', 'push', 'codeberg', '--all', '--force'], cwd=repo_path)
                        if not success:
                            log(f"  强制推送失败: {output[:100]}")
                            return False
            else:
                # 创建新仓库
                log(f"  创建新仓库: {codeberg_name}")
                if self.create_codeberg_repo(codeberg_name, repo_info.get('description', '')):
                    success, output = run_command(['git', 'push', 'codeberg', '--all', '--force'], cwd=repo_path)
                    if not success:
                        log(f"  创建后推送失败: {output[:100]}")
                        return False
                else:
                    return False
            
            # 推送标签
            log(f"  推送标签...")
            run_command(['git', 'push', 'codeberg', '--tags', '--force'], cwd=repo_path)
            
            # 更新状态
            self.state[repo_full_name] = {
                'name': original_name,
                'codeberg_name': codeberg_name,
                'last_updated': repo_info['updated_at'],
                'last_synced': datetime.now().isoformat(),
                'operation': operation,
                'renamed': is_renamed
            }
            
            log(f"✓ 同步完成，Codeberg仓库: {codeberg_name}")
            return True
            
        except Exception as e:
            log(f"✗ 同步出错: {e}")
            return False
    
    def _handle_repo_conflict(self, repo_info: Dict, repo_path: Path, original_name: str, is_renamed: bool) -> bool:
        """处理仓库冲突"""
        repo_full_name = repo_info['full_name']
        
        if is_renamed:
            # 已经是重命名后的仓库，但仍有冲突，尝试强制推送
            log(f"  已经是重命名后的仓库，尝试强制推送")
            success, output = run_command(['git', 'push', 'codeberg', '--all', '--force'], cwd=repo_path)
            if success:
                return True
            else:
                log(f"  强制推送失败: {output[:100]}")
                return False
        
        # 生成新的唯一名称
        owner = repo_full_name.split('/')[0]
        new_codeberg_name = f"{owner}-{original_name}"
        
        # 检查新名称是否已被使用
        if self.codeberg_repo_exists(new_codeberg_name) or self.is_name_already_used(new_codeberg_name, repo_full_name):
            # 如果新名称也存在，添加哈希后缀
            hash_suffix = hashlib.md5(repo_full_name.encode()).hexdigest()[:6]
            new_codeberg_name = f"{new_codeberg_name}-{hash_suffix}"
        
        # 更新远程URL
        new_codeberg_url = f"https://{self.codeberg_username}:{self.codeberg_token}@codeberg.org/{self.codeberg_username}/{new_codeberg_name}.git"
        run_command(['git', 'remote', 'set-url', 'codeberg', new_codeberg_url], cwd=repo_path)
        
        # 创建新仓库并推送
        if self.create_codeberg_repo(new_codeberg_name, repo_info.get('description', '')):
            success, output = run_command(['git', 'push', 'codeberg', '--all', '--force'], cwd=repo_path)
            if success:
                # 更新状态
                self.state[repo_full_name] = {
                    'name': original_name,
                    'codeberg_name': new_codeberg_name,
                    'last_updated': repo_info['updated_at'],
                    'last_synced': datetime.now().isoformat(),
                    'operation': 'cloned',
                    'renamed': True
                }
                self.stats['renamed'] += 1
                log(f"  重命名为: {new_codeberg_name}")
                return True
            else:
                log(f"  重命名后推送失败: {output[:100]}")
                return False
        else:
            log(f"  创建新仓库失败")
            return False
    
    def run(self):
        """主运行函数"""
        log("=" * 60)
        log("开始同步starred仓库到Codeberg")
        log(f"Codeberg用户名: {self.codeberg_username}")
        log("=" * 60)
        
        # 获取仓库列表
        repos = self.get_starred_repos()
        self.stats['total'] = len(repos)
        
        for i, repo in enumerate(repos, 1):
            repo_full_name = repo['full_name']
            
            log(f"[{i}/{len(repos)}] 处理: {repo_full_name}")
            
            if not self.should_sync_repo(repo):
                log(f"  跳过（未更新）")
                self.stats['skipped'] += 1
                continue
            
            if self.sync_repository(repo):
                if self.state[repo_full_name]['operation'] == 'cloned':
                    self.stats['new'] += 1
                else:
                    self.stats['updated'] += 1
            else:
                self.stats['failed'] += 1
            
            # 每处理3个仓库保存一次状态
            if i % 3 == 0:
                log("保存中间状态...")
                self.save_state()
            
            time.sleep(1)
        
        self.save_state()
        
        # 显示统计
        log("=" * 60)
        log("同步完成!")
        log(f"总计仓库: {self.stats['total']}")
        log(f"新增仓库: {self.stats['new']}")
        log(f"更新仓库: {self.stats['updated']}")
        log(f"重命名仓库: {self.stats['renamed']}")
        log(f"跳过仓库: {self.stats['skipped']}")
        log(f"失败仓库: {self.stats['failed']}")
        
        duration = datetime.now() - self.stats['start_time']
        minutes = int(duration.total_seconds() // 60)
        seconds = int(duration.total_seconds() % 60)
        log(f"总耗时: {minutes}分{seconds}秒")
        log("=" * 60)
        
        if self.stats['failed'] > 0:
            sys.exit(1)

def main():
    try:
        sync = SyncManager()
        sync.run()
    except KeyboardInterrupt:
        log("\n操作被用户中断")
        sys.exit(1)
    except Exception as e:
        log(f"发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
