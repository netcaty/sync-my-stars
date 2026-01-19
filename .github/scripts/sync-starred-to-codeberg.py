#!/usr/bin/env python3
"""
同步 GitHub starred 仓库到 Codeberg
状态文件保存在 .github/sync_state.json
"""

import os
import sys
import json
import requests
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import hashlib

def log(message: str):
    """简单的日志函数"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        if result.returncode != 0:
            return False, f"{result.stderr}"
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, f"命令超时: {' '.join(cmd)}"
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
        
        # 工作目录（GitHub Actions 工作区）
        self.workspace = Path('.')
        
        # 状态文件路径（保存在项目根目录的 .github 文件夹中）
        self.state_file = self.workspace / '.github' / 'sync_state.json'
        self.state_file.parent.mkdir(exist_ok=True)  # 确保目录存在
        
        # 临时存储仓库的目录
        self.repos_dir = Path('/tmp/github-backup/repos')
        self.repos_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载状态
        self.state = self.load_state()
        
        # 统计信息
        self.stats = {
            'total': 0,
            'new': 0,
            'updated': 0,
            'skipped': 0,
            'failed': 0,
            'start_time': datetime.now()
        }
    
    def load_state(self) -> Dict:
        """从项目根目录加载状态文件"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state_data = json.load(f)
                    log(f"从 {self.state_file} 加载状态，包含 {len(state_data)} 个仓库记录")
                    return state_data
            except json.JSONDecodeError as e:
                log(f"警告: 状态文件格式错误，创建新的: {e}")
                return {}
            except Exception as e:
                log(f"警告: 加载状态文件失败: {e}")
                return {}
        else:
            log("状态文件不存在，创建新的")
            return {}
    
    def save_state(self):
        """保存状态文件到项目根目录"""
        try:
            # 准备要保存的数据
            data_to_save = {
                'metadata': {
                    'last_updated': datetime.now().isoformat(),
                    'github_user': self.get_github_username(),
                    'total_repos': len(self.state)
                },
                'repositories': self.state
            }
            
            # 保存到文件
            with open(self.state_file, 'w') as f:
                json.dump(data_to_save, f, indent=2, ensure_ascii=False)
            
            log(f"状态已保存到: {self.state_file}")
            
        except Exception as e:
            log(f"错误: 保存状态文件失败: {e}")
            # 在失败时尝试保存简化版本
            try:
                with open(self.state_file, 'w') as f:
                    json.dump(self.state, f, indent=2)
                log("已保存简化版本的状态文件")
            except Exception as e2:
                log(f"保存简化版本也失败: {e2}")
    
    def get_github_username(self) -> str:
        """获取 GitHub 用户名"""
        try:
            response = requests.get(
                'https://api.github.com/user',
                headers=self.github_headers
            )
            if response.status_code == 200:
                return response.json().get('login', 'unknown')
        except Exception:
            pass
        return 'unknown'
    
    def get_starred_repos(self) -> List[Dict]:
        """获取所有 starred 仓库（处理分页）"""
        repos = []
        page = 1
        per_page = 100  # GitHub API 每页最大数量
        
        log("获取 GitHub starred 仓库列表...")
        
        while True:
            try:
                url = f"https://api.github.com/user/starred"
                params = {
                    'page': page,
                    'per_page': per_page,
                    'sort': 'updated',
                    'direction': 'desc'
                }
                
                response = requests.get(
                    url, 
                    headers=self.github_headers,
                    params=params,
                    timeout=30
                )
                
                if response.status_code != 200:
                    log(f"获取 starred 仓库失败: {response.status_code}")
                    log(f"响应: {response.text[:200]}")
                    break
                
                page_repos = response.json()
                if not page_repos:
                    break
                
                for repo_data in page_repos:
                    repos.append({
                        'full_name': repo_data['full_name'],
                        'name': repo_data['name'],
                        'description': repo_data.get('description', '')[:200],
                        'updated_at': repo_data['updated_at'],
                        'clone_url': repo_data['clone_url'],
                        'size': repo_data.get('size', 0),
                        'language': repo_data.get('language', 'Unknown')
                    })
                
                log(f"已获取第 {page} 页，共 {len(repos)} 个仓库")
                
                # 检查是否还有更多页面
                link_header = response.headers.get('Link', '')
                if 'rel="next"' not in link_header:
                    break
                    
                page += 1
                time.sleep(0.5)  # 避免速率限制
                
            except requests.exceptions.RequestException as e:
                log(f"网络请求失败: {e}")
                break
            except Exception as e:
                log(f"处理仓库数据时出错: {e}")
                break
        
        log(f"总共获取到 {len(repos)} 个 starred 仓库")
        return repos
    
    def should_sync_repo(self, repo_info: Dict) -> bool:
        """判断是否需要同步仓库"""
        # 如果是强制完整同步，则总是同步
        if self.full_sync:
            return True
        
        repo_full_name = repo_info['full_name']
        repo_updated = repo_info['updated_at']
        
        # 检查状态文件中是否有记录
        if repo_full_name in self.state:
            repo_state = self.state[repo_full_name]
            last_synced = repo_state.get('last_updated')
            
            # 如果仓库未更新，则跳过
            if last_synced == repo_updated:
                return False
        
        return True
    
    def codeberg_repo_exists(self, repo_name: str) -> bool:
        """检查 Codeberg 上是否已存在仓库"""
        try:
            url = f"https://codeberg.org/api/v1/repos/{self.codeberg_username}/{repo_name}"
            response = requests.get(url, headers=self.codeberg_headers, timeout=10)
            return response.status_code == 200
        except Exception as e:
            log(f"检查 Codeberg 仓库失败: {e}")
            return False
    
    def create_codeberg_repo(self, repo_info: Dict) -> bool:
        """在 Codeberg 上创建仓库"""
        repo_name = repo_info['name']
        description = repo_info['description'] or f"Mirror of {repo_info['full_name']}"
        
        try:
            url = f"https://codeberg.org/api/v1/user/repos"
            data = {
                'name': repo_name,
                'description': description[:255],
                'private': False,
                'auto_init': False
            }
            
            response = requests.post(
                url, 
                headers=self.codeberg_headers, 
                json=data,
                timeout=30
            )
            
            if response.status_code == 201:
                log(f"✓ 在 Codeberg 创建仓库: {repo_name}")
                return True
            elif response.status_code == 409:
                log(f"✓ 仓库已存在: {repo_name}")
                return True
            else:
                log(f"✗ 创建仓库失败 {response.status_code}: {response.text[:100]}")
                return False
                
        except Exception as e:
            log(f"✗ 创建 Codeberg 仓库时出错: {e}")
            return False
    
    def sync_repository(self, repo_info: Dict) -> Tuple[bool, str]:
        """同步单个仓库"""
        repo_full_name = repo_info['full_name']
        repo_name = repo_info['name']
        repo_path = self.repos_dir / repo_name
        
        try:
            operation = "updated"
            
            # 克隆或更新
            if repo_path.exists() and (repo_path / 'config').exists():
                # 增量更新
                log(f"  增量更新仓库...")
                success, output = run_command(['git', 'fetch', '--all', '--prune'], cwd=repo_path)
                if not success:
                    log(f"  更新失败: {output[:200]}")
                    return False, "fetch_failed"
            else:
                # 完整克隆
                log(f"  克隆新仓库...")
                # 使用 GitHub token 进行认证
                auth_clone_url = f"https://{self.github_token}@github.com/{repo_full_name}.git"
                success, output = run_command(['git', 'clone', '--mirror', auth_clone_url, str(repo_path)])
                if not success:
                    log(f"  克隆失败: {output[:200]}")
                    return False, "clone_failed"
                operation = "cloned"
            
            # 确保 Codeberg 仓库存在
            if not self.codeberg_repo_exists(repo_name):
                if not self.create_codeberg_repo(repo_info):
                    return False, "create_codeberg_failed"
            
            # 推送到 Codeberg
            log(f"  推送到 Codeberg...")
            codeberg_url = f"https://{self.codeberg_username}:{self.codeberg_token}@codeberg.org/{self.codeberg_username}/{repo_name}.git"
            
            # 设置或更新远程
            success, output = run_command(['git', 'remote'], cwd=repo_path)
            if success:
                if 'codeberg' not in output:
                    # 添加远程
                    success, output = run_command(['git', 'remote', 'add', 'codeberg', codeberg_url], cwd=repo_path)
                else:
                    # 更新远程 URL
                    success, output = run_command(['git', 'remote', 'set-url', 'codeberg', codeberg_url], cwd=repo_path)
            
            if not success:
                log(f"  设置远程失败: {output[:200]}")
                return False, "remote_setup_failed"
            
            # 推送所有分支和标签
            success, output = run_command(['git', 'push', '--mirror', 'codeberg'], cwd=repo_path)
            if not success:
                log(f"  推送失败: {output[:200]}")
                return False, "push_failed"
            
            # 更新状态记录
            self.state[repo_full_name] = {
                'name': repo_name,
                'last_updated': repo_info['updated_at'],
                'last_synced': datetime.now().isoformat(),
                'operation': operation,
                'size_mb': repo_info.get('size', 0),
                'language': repo_info.get('language', 'Unknown')
            }
            
            log(f"✓ 同步完成: {repo_name}")
            return True, operation
            
        except Exception as e:
            log(f"✗ 同步出错: {str(e)[:200]}")
            return False, "exception"
    
    def cleanup_old_repos(self, current_repos: List[Dict]):
        """清理已取消 star 的仓库状态"""
        current_names = {repo['full_name'] for repo in current_repos}
        removed_count = 0
        
        for repo_name in list(self.state.keys()):
            if repo_name not in current_names:
                del self.state[repo_name]
                removed_count += 1
        
        if removed_count > 0:
            log(f"清理了 {removed_count} 个已取消 star 的仓库状态")
    
    def run(self):
        """主运行函数"""
        log("=" * 60)
        log("开始同步 starred 仓库到 Codeberg")
        log("状态文件: .github/sync_state.json")
        log(f"临时目录: {self.repos_dir}")
        log("=" * 60)
        
        # 获取仓库列表
        repos = self.get_starred_repos()
        self.stats['total'] = len(repos)
        
        if not repos:
            log("没有找到 starred 仓库")
            return
        
        # 清理已取消 star 的仓库状态
        self.cleanup_old_repos(repos)
        
        log(f"找到 {len(repos)} 个仓库需要同步")
        log("-" * 60)
        
        # 同步每个仓库
        for i, repo in enumerate(repos, 1):
            repo_full_name = repo['full_name']
            
            log(f"[{i}/{len(repos)}] 处理: {repo_full_name}")
            
            # 检查是否需要同步
            if not self.should_sync_repo(repo):
                log(f"  仓库未更新，跳过")
                self.stats['skipped'] += 1
                continue
            
            # 执行同步
            success, operation = self.sync_repository(repo)
            
            if success:
                if operation == "cloned":
                    self.stats['new'] += 1
                else:
                    self.stats['updated'] += 1
            else:
                self.stats['failed'] += 1
            
            # 每处理 3 个仓库保存一次状态（防止中途失败丢失所有进度）
            if i % 3 == 0:
                log("保存中间状态...")
                self.save_state()
            
            # 添加延迟避免速率限制
            time.sleep(1.5)
        
        # 最终保存状态
        self.save_state()
        
        # 显示统计
        log("=" * 60)
        log("同步完成!")
        log(f"总计仓库: {self.stats['total']}")
        log(f"新增仓库: {self.stats['new']}")
        log(f"更新仓库: {self.stats['updated']}")
        log(f"跳过仓库: {self.stats['skipped']}")
        log(f"失败仓库: {self.stats['failed']}")
        
        duration = datetime.now() - self.stats['start_time']
        minutes = int(duration.total_seconds() // 60)
        seconds = int(duration.total_seconds() % 60)
        log(f"总耗时: {minutes}分{seconds}秒")
        log("=" * 60)
        
        # 如果有失败，返回错误码
        if self.stats['failed'] > 0:
            sys.exit(1)

def main():
    """主函数"""
    try:
        sync = SyncManager()
        sync.run()
    except KeyboardInterrupt:
        log("\n操作被用户中断")
        sys.exit(1)
    except Exception as e:
        log(f"发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()