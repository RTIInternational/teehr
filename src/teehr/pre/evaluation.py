from typing import Union
from pathlib import Path
import shutil
import logging

logger = logging.getLogger(__name__)


def copy_template_to(
        template_dir: Union[str, Path],
        destination_dir: Union[str, Path]
):
    """Copy the template directory to the destination directory."""
    template_dir = Path(template_dir)
    destination_dir = Path(destination_dir)

    for file in template_dir.glob('**/*'):
        if file.is_dir():
            destination_file = Path(
                destination_dir,
                file.relative_to(template_dir)
            )
            if not destination_file.parent.is_dir():
                destination_file.parent.mkdir(parents=True)
            logger.debug(f"Making directory {destination_file}")
            destination_file.mkdir()
        if file.is_file():
            destination_file = Path(
                destination_dir,
                file.relative_to(template_dir)
            )
            if not destination_file.parent.is_dir():
                destination_file.parent.mkdir(parents=True)
            logger.debug(f"Copying file {file} to {destination_file}")
            shutil.copy(file, destination_file)

    logger.debug(
        f"Renaming {destination_dir}/gitignore_template to .gitignore"
    )

    Path(destination_dir, "gitignore_template").rename(Path(destination_dir, ".gitignore"))
