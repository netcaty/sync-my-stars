#!/usr/bin/env python3
"""
GitHub Actions 专用脚本：同步 GitHub starred 仓库到 Codeberg
支持增量同步和错误恢复
"""

import os
import sys
import json
import yaml
import requests
import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import tempfile

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'sync_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GitHubCodebergSync:
    def __init__(self):
        # 从环境变量读取配置
        self.github_token = os.getenv('GITHUB_TOKEN')
        self.codeberg_username = os.getenv('CODEBERG_USERNAME')
        self.codeberg_token = os.getenv('CODEBERG_TOKEN')
        self.full_sync = os.getenv('FULL_SYNC', 'false').lower() == 'true'
        
        # 验证必要的配置
        if not all([self.github_token, self.codeberg_username, self.codeberg_token]):
            missing = []
            if not self.github_token: missing.append('GITHUB_TOKEN')
            if not self.codeberg_username: missing.append('CODEBERG_USERNAME')
            if not self.codeberg_token: missing.append('CODEBERG_TOKEN')
            raise ValueError(f"缺少必要的环境变量: {', '.join(missing)}")
        
        # GitHub API 配置
        self.github_headers = {
            'Authorization': f'token {self.github_token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        # Codeberg API 配置
        self.codeberg_headers = {
            'Authorization': f'token {self.codeberg_token}',
            'Content-Type': 'application/json'
        }
        
        # 统计信息
        self.stats = {
            'start_time': datetime.now().isoformat(),
            'total_repos': 0,
            'new_repos': 0,
            'updated_repos': 0,
            'skipped_repos': 0,
            'failed_repos': 0,
            'repositories': []
        }
        
        # 在 GitHub Actions 中使用临时目录
        self.workspace = Path(os.getenv('GITHUB_WORKSPACE', '/tmp'))
        self.repos_dir = self.workspace / 'repos'
        self.repos_dir.mkdir(exist_ok=True)
        
        # 状态文件路径（用于增量同步）
        self.state_file = self.workspace / 'sync_state.json'
        
    def run_command(self, cmd: List[str], cwd: Optional[Path] = None) -> Tuple[bool, str]:
        """执行 shell 命令并返回结果"""
        try:
            result = subprocess.run(
                cmd,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=300  # 5分钟超时
            )
            if result.returncode != 0:
                return False, f"命令失败: {' '.join(cmd)}\n错误: {result.stderr}"
            return True, result.stdout.strip()
        except subprocess.TimeoutExpired:
            return False, f"命令超时: {' '.join(cmd)}"
        except Exception as e:
            return False, f"执行命令时出错: {str(e)}"
    
    def get_github_starred_repos(self) -> List[Dict]:
        """获取 GitHub starred 仓库列表（支持分页）"""
        repos = []
        page = 1
        per_page = 100
        
        logger.info("开始获取 GitHub starred 仓库列表...")
        
        while True:
            url = f"https://api.github.com/user/starred?page={page}&per_page={per_page}"
            response = requests.get(url, headers=self.github_headers)
            
            if response.status_code != 200:
                logger.error(f"获取 starred 仓库失败: {response.status_code} - {response.text}")
                break
            
            page_repos = response.json()
            if not page_repos:
                break
                
            for repo in page_repos:
                repos.append({
                    'full_name': repo['full_name'],
                    'name': repo['name'],
                    'description': repo.get('description', ''),
                    'html_url': repo['html_url'],
                    'updated_at': repo['updated_at'],
                    'clone_url': repo['clone_url']
                })
            
            logger.info(f"已获取第 {page} 页，共 {len(repos)} 个仓库")
            page += 1
            
            # 检查是否还有更多页面
            if 'link' in response.headers:
                links = response.headers['link']
                if 'rel="next"' not in links:
                    break
            else:
                # 如果没有 Link 头，假设这是最后一页
                break
        
        logger.info(f"总共获取到 {len(repos)} 个 starred 仓库")
        return repos
    
    def codeberg_repo_exists(self, repo_name: str) -> bool:
        """检查 Codeberg 上是否已存在仓库"""
        url = f"https://codeberg.org/api/v1/repos/{self.codeberg_username}/{repo_name}"
        response = requests.get(url, headers=self.codeberg_headers)
        return response.status_code == 200
    
    def create_codeberg_repo(self, repo_name: str, description: str = "") -> bool:
        """在 Codeberg 上创建仓库"""
        url = f"https://codeberg.org/api/v1/user/repos"
        data = {
            'name': repo_name,
            'description': description[:255] if description else f"Mirror of {repo_name} from GitHub",
            'private': False,
            'auto_init': False
        }
        
        response = requests.post(url, headers=self.codeberg_headers, json=data)
        
        if response.status_code == 201:
            logger.info(f"在 Codeberg 上创建仓库成功: {repo_name}")
            return True
        elif response.status_code == 409:
            logger.info(f"仓库已存在: {repo_name}")
            return True
        else:
            logger.error(f"创建仓库失败: {response.status_code} - {response.text}")
            return False
    
    def git_mirror_clone(self, repo_url: str, local_path: Path) -> bool:
        """使用 git mirror 克隆仓库"""
        logger.info(f"镜像克隆仓库: {repo_url}")
        
        # 如果目录已存在，清理后重新克隆
        if local_path.exists():
            success, output = self.run_command(['rm', '-rf', str(local_path)])
            if not success:
                logger.error(f"清理目录失败: {output}")
                return False
        
        # 使用 git clone --mirror
        cmd = ['git', 'clone', '--mirror', repo_url, str(local_path)]
        success, output = self.run_command(cmd)
        
        if not success:
            logger.error(f"镜像克隆失败: {output}")
            return False
        
        logger.info(f"镜像克隆成功: {local_path.name}")
        return True
    
    def git_incremental_update(self, local_path: Path) -> bool:
        """增量更新已存在的仓库"""
        logger.info(f"增量更新仓库: {local_path.name}")
        
        # 获取远程更新
        fetch_cmd = ['git', 'fetch', '--all', '--prune']
        success, output = self.run_command(fetch_cmd, cwd=local_path)
        
        if not success:
            logger.error(f"获取更新失败: {output}")
            return False
        
        logger.info(f"增量更新成功: {local_path.name}")
        return True
    
    def push_to_codeberg(self, local_path: Path, repo_name: str) -> bool:
        """推送到 Codeberg"""
        logger.info(f"推送仓库到 Codeberg: {repo_name}")
        
        # 准备 Codeberg 远程 URL
        codeberg_url = f"https://{self.codeberg_username}:{self.codeberg_token}@codeberg.org/{self.codeberg_username}/{repo_name}.git"
        
        # 检查是否已添加远程
        remote_cmd = ['git', 'remote']
        success, output = self.run_command(remote_cmd, cwd=local_path)
        
        if success and 'codeberg' in output:
            # 更新远程 URL
            set_url_cmd = ['git', 'remote', 'set-url', 'codeberg', codeberg_url]
        else:
            # 添加远程
            set_url_cmd = ['git', 'remote', 'add', 'codeberg', codeberg_url]
        
        success, output = self.run_command(set_url_cmd, cwd=local_path)
        if not success:
            logger.error(f"设置远程失败: {output}")
            return False
        
        # 推送到 Codeberg
        push_cmd = ['git', 'push', '--mirror', 'codeberg']
        success, output = self.run_command(push_cmd, cwd=local_path)
        
        if not success:
            logger.error(f"推送失败: {output}")
            return False
        
        logger.info(f"推送成功: {repo_name}")
        return True
    
    def load_sync_state(self) -> Dict:
        """加载同步状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载状态文件失败: {e}")
        return {}
    
    def save_sync_state(self, state: Dict):
        """保存同步状态"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"保存状态文件失败: {e}")
    
    def process_repository(self, repo_info: Dict, state: Dict) -> bool:
        """处理单个仓库"""
        repo_name = repo_info['name']
        repo_full_name = repo_info['full_name']
        repo_updated = repo_info['updated_at']
        
        logger.info(f"处理仓库: {repo_full_name}")
        
        # 本地目录
        local_path = self.repos_dir / repo_name
        
        # 检查是否需要同步（除非是完整同步）
        need_sync = self.full_sync
        
        if not need_sync:
            repo_state = state.get(repo_full_name, {})
            last_synced = repo_state.get('last_updated')
            if last_synced and last_synced == repo_updated:
                logger.info(f"仓库未更新，跳过: {repo_full_name}")
                self.stats['skipped_repos'] += 1
                return True
        
        try:
            # 克隆或更新仓库
            if local_path.exists() and (local_path / 'config').exists():
                if not self.git_incremental_update(local_path):
                    return False
            else:
                if not self.git_mirror_clone(repo_info['clone_url'], local_path):
                    return False
            
            # 确保 Codeberg 仓库存在
            if not self.codeberg_repo_exists(repo_name):
                logger.info(f"在 Codeberg 上创建仓库: {repo_name}")
                if not self.create_codeberg_repo(repo_name, repo_info['description']):
                    return False
            
            # 推送到 Codeberg
            if not self.push_to_codeberg(local_path, repo_name):
                return False
            
            # 更新状态
            state[repo_full_name] = {
                'last_updated': repo_updated,
                'synced_at': datetime.now().isoformat(),
                'name': repo_name
            }
            
            # 更新统计
            if local_path.exists() and (local_path / 'config').exists():
                self.stats['updated_repos'] += 1
            else:
                self.stats['new_repos'] += 1
            
            logger.info(f"仓库处理完成: {repo_full_name}")
            return True
            
        except Exception as e:
            logger.error(f"处理仓库 {repo_full_name} 时出错: {str(e)}")
            self.stats['failed_repos'] += 1
            return False
    
    def run(self):
        """主运行方法"""
        logger.info("开始同步 starred 仓库到 Codeberg")
        logger.info(f"工作目录: {self.workspace}")
        logger.info(f"完整同步模式: {self.full_sync}")
        
        # 加载之前的同步状态
        state = self.load_sync_state()
        
        # 获取 starred 仓库列表
        try:
            repos = self.get_github_starred_repos()
            self.stats['total_repos'] = len(repos)
        except Exception as e:
            logger.error(f"获取仓库列表失败: {e}")
            sys.exit(1)
        
        # 处理每个仓库
        for repo_info in repos:
            success = self.process_repository(repo_info, state)
            
            # 保存状态（每处理10个仓库保存一次）
            if len(state) % 10 == 0:
                self.save_sync_state(state)
            
            # 添加延迟以避免速率限制
            time.sleep(1)
        
        # 最终保存状态
        self.save_sync_state(state)
        
        # 完成统计
        self.stats['end_time'] = datetime.now().isoformat()
        self.stats['duration'] = (datetime.fromisoformat(self.stats['end_time']) - 
                                  datetime.fromisoformat(self.stats['start_time'])).total_seconds()
        
        # 保存统计信息
        stats_file = self.workspace / 'sync_stats.json'
        with open(stats_file, 'w') as f:
            json.dump(self.stats, f, indent=2)
        
        # 输出摘要
        logger.info("\n" + "="*50)
        logger.info("同步完成!")
        logger.info(f"总计仓库: {self.stats['total_repos']}")
        logger.info(f"新增仓库: {self.stats['new_repos']}")
        logger.info(f"更新仓库: {self.stats['updated_repos']}")
        logger.info(f"跳过仓库: {self.stats['skipped_repos']}")
        logger.info(f"失败仓库: {self.stats['failed_repos']}")
        logger.info(f"总耗时: {self.stats['duration']:.2f} 秒")
        logger.info("="*50)
        
        # 如果有失败的仓库，返回错误码
        if self.stats['failed_repos'] > 0:
            sys.exit(1)

def main():
    """主函数"""
    try:
        sync = GitHubCodebergSync()
        sync.run()
    except Exception as e:
        logger.error(f"同步过程发生错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()